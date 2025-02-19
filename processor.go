// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cohere

import (
	"context"
	"fmt"
	"strings"
	"time"

	cohereClient "github.com/cohere-ai/cohere-go/v2/client"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/jpillora/backoff"
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

type Processor struct {
	sdk.UnimplementedProcessor

	responseBodyRef *sdk.ReferenceResolver

	config     ProcessorConfig
	backoffCfg *backoff.Backoff
	client     *cohereClient.Client
}

const (
	CommandModel = "command"
	EmbedModel   = "embed"
	RerankModel  = "rerank"
)

// Allowed values for embeddingTypes.
var allowedEmbeddingTypes = map[string]bool{
	"float":   true,
	"int8":    true,
	"uint8":   true,
	"binary":  true,
	"ubinary": true,
}

type ProcessorConfig struct {
	// Model is one of the Cohere model (command,embed,rerank).
	Model string `json:"model" validate:"required" default:"command"`
	// ModelVersion is version of one of the models (command,embed,rerank).
	ModelVersion string `json:"modelVersion" validate:"required" default:"command"`
	// APIKey is the API key for Cohere api calls.
	APIKey string `json:"apiKey" validate:"required"`
	// Maximum number of retries for an individual record when backing off following an error.
	BackoffRetryCount float64 `json:"backoffRetry.count" default:"0" validate:"gt=-1"`
	// The multiplying factor for each increment step.
	BackoffRetryFactor float64 `json:"backoffRetry.factor" default:"2" validate:"gt=0"`
	// The minimum waiting time before retrying.
	BackoffRetryMin time.Duration `json:"backoffRetry.min" default:"100ms"`
	// The maximum waiting time before retrying.
	BackoffRetryMax time.Duration `json:"backoffRetry.max" default:"5s"`
	// Specifies in which field should the response body be saved.
	ResponseBodyRef string `json:"response.body" validate:"required" default:".Payload.After"`

	// Embed-specific configurations
	EmbedConfig *EmbedConfig `json:"embedConfig"`
}

type EmbedConfig struct {
	// Specifies the type of input passed to the model. Required for embedding models v3 and higher.
	InputType string `json:"inputType" validate:"inclusion=search_document|search_query|classification|clustering|image"`
	// Specifies the types of embeddings you want to get back. Can be one or more of the allowed types.
	EmbeddingTypes []string `json:"embeddingTypes"`
	// Handles input exceeding max token length: START trims from the beginning, END from the end, NONE returns an error.
	Truncate string `json:"truncate" default:"NONE" validate:"inclusion=NONE|START|END"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c ProcessorConfig) Validate() error {
	// validate `embedConfig` if model is "embed".
	if c.Model == EmbedModel {
		if c.EmbedConfig == nil {
			return fmt.Errorf("embedConfig is required when model is 'embed'")
		}

		if len(c.EmbedConfig.EmbeddingTypes) == 0 {
			return fmt.Errorf("atleast one embeddingType must be provided")
		}

		// validate `inputType` for v3 models.
		if strings.Contains(c.ModelVersion, "v3") && c.EmbedConfig.InputType == "" {
			return fmt.Errorf("inputType required for embedding models v3 and higher")
		}

		// validate each `embeddingType`.
		for _, et := range c.EmbedConfig.EmbeddingTypes {
			if _, ok := allowedEmbeddingTypes[et]; !ok {
				return fmt.Errorf("invalid embeddingType: %s. Allowed values: float, int8, uint8, binary, ubinary", et)
			}
		}
	}

	return nil
}

func NewProcessor() sdk.Processor {
	// Create Processor and wrap it in the default middleware.
	return sdk.ProcessorWithMiddleware(&Processor{}, sdk.DefaultProcessorMiddleware()...)
}

func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	// Configure is the first function to be called in a processor. It provides the processor
	// with the configuration that needs to be validated and stored to be used in other methods.
	// This method should not open connections or any other resources. It should solely focus
	// on parsing and validating the configuration itself.

	err := sdk.ParseConfig(ctx, cfg, &p.config, ProcessorConfig{}.Parameters())
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	err = p.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	responseBodyRef, err := sdk.NewReferenceResolver(p.config.ResponseBodyRef)
	if err != nil {
		return fmt.Errorf("failed parsing response.body %v: %w", p.config.ResponseBodyRef, err)
	}
	p.responseBodyRef = &responseBodyRef

	// new cohere client
	p.client = cohereClient.NewClient()

	p.backoffCfg = &backoff.Backoff{
		Factor: p.config.BackoffRetryFactor,
		Min:    p.config.BackoffRetryMin,
		Max:    p.config.BackoffRetryMax,
	}

	return nil
}

func (p *Processor) Specification() (sdk.Specification, error) {
	// Specification contains the metadata for the processor, which can be used to define how
	// to reference the processor, describe what the processor does and the configuration
	// parameters it expects.

	return sdk.Specification{
		Name:        "conduit-processor-cohere",
		Summary:     "Conduit processor for Cohere's models.",
		Description: "Conduit processor for Cohere's models Command, Embed and Rerank.",
		Version:     "devel",
		Author:      "Conduit",
		Parameters:  p.config.Parameters(),
	}, nil
}

func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	// Process is the main show of the processor, here we would manipulate the records received
	// and return the processed ones. After processing the slice of records that the function
	// got, and if no errors occurred, it should return a slice of sdk.ProcessedRecord that
	// matches the length of the input slice. However, if an error occurred while processing a
	// specific record, then it should be reflected in the ProcessedRecord with the same index
	// as the input record, and should return the slice at that index length.

	processedRecords := []sdk.ProcessedRecord{}
	switch p.config.Model {
	case CommandModel:
		processedRecords = p.processCommandModel(ctx, records)

	case EmbedModel:
		processedRecords = p.processEmbedModel(ctx, records)

	case RerankModel:
		processedRecords = p.processRerankModel(ctx, records)

	default:
		sdk.Logger(ctx).Info().Msg("unknown cohere model")
	}

	return processedRecords
}

func (p *Processor) setField(r *opencdc.Record, refRes *sdk.ReferenceResolver, data any) error {
	if refRes == nil {
		return nil
	}

	ref, err := refRes.Resolve(r)
	if err != nil {
		return fmt.Errorf("error reference resolver: %w", err)
	}

	err = ref.Set(data)
	if err != nil {
		return fmt.Errorf("error reference set: %w", err)
	}

	return nil
}
