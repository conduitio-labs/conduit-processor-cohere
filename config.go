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
	"fmt"
	"strings"
	"time"
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

// allowed values for embeddingTypes.
var allowedEmbeddingTypes = map[string]bool{
	"float":   true,
	"int8":    true,
	"uint8":   true,
	"binary":  true,
	"ubinary": true,
}

// allowed values for inputType.
var allowedInputTypes = map[string]bool{
	"search_document": true,
	"search_query":    true,
	"classification":  true,
	"clustering":      true,
	"image":           true,
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
	ResponseBodyRef string `json:"response.body" default:".Payload.After"`
	// Config specific to embed model
	EmbedConfig *EmbedConfig `json:"embedConfig"`
}

type EmbedConfig struct {
	// Specifies the type of input passed to the model. Required for embedding models v3 and higher.
	// Allowed values: search_document, search_query, classification, clustering, image.
	InputType string `json:"inputType"`
	// Specifies the types of embeddings you want to get back. Can be one or more of the allowed values.
	// Allowed values: float, int8, uint8, binary, ubinary.
	EmbeddingTypes []string `json:"embeddingTypes"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c ProcessorConfig) Validate() error {
	if c.Model == EmbedModel {
		if err := validateEmbedModel(c); err != nil {
			return err
		}
	}
	return nil
}

// validateEmbedModel validates configurations specific to the embed model.
func validateEmbedModel(c ProcessorConfig) error {
	// validate `modelVersion` for embed model.
	if !strings.Contains(c.ModelVersion, EmbedModel) {
		return fmt.Errorf("modelVersion does not belong to provided model")
	}

	// ensure `embedConfig` is provided.
	if c.EmbedConfig == nil {
		return fmt.Errorf("embedConfig is required when model is 'embed'")
	}

	// validate `inputType` for v3 models.
	if strings.Contains(c.ModelVersion, "v3") && (c.EmbedConfig.InputType == "" || !allowedInputTypes[c.EmbedConfig.InputType]) {
		return fmt.Errorf("invalid or missing inputType for v3 models: %s", c.EmbedConfig.InputType)
	}

	// ensure `embeddingTypes` is provided.
	if len(c.EmbedConfig.EmbeddingTypes) == 0 {
		return fmt.Errorf("atleast one embeddingType must be provided")
	}

	// validate `embeddingTypes`.
	for _, et := range c.EmbedConfig.EmbeddingTypes {
		if _, ok := allowedEmbeddingTypes[et]; !ok {
			return fmt.Errorf("invalid embeddingType: %s", et)
		}
	}

	return nil
}
