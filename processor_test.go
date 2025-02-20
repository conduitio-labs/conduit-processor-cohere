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
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

func TestProcessor_Configure(t *testing.T) {
	tests := []struct {
		name    string
		config  config.Config
		wantErr string
	}{
		{
			name:    "empty config returns error",
			config:  config.Config{},
			wantErr: `failed to parse configuration: config invalid: error validating "apiKey": required parameter is not provided`,
		},
		{
			name: "invalid backoffRetry.count returns error",
			config: config.Config{
				"apiKey":             "api-key",
				"model":              "command",
				"modelVersion":       "command",
				"backoffRetry.count": "not-a-number",
			},
			wantErr: `failed to parse configuration: config invalid: error validating "backoffRetry.count": "not-a-number" value is not a float: invalid parameter type`,
		},
		{
			name: "invalid backoffRetry.min returns error",
			config: config.Config{
				"apiKey":             "api-key",
				"model":              "command",
				"modelVersion":       "command",
				"backoffRetry.count": "1",
				"backoffRetry.min":   "not-a-duration",
			},
			wantErr: `failed to parse configuration: config invalid: error validating "backoffRetry.min": "not-a-duration" value is not a duration: invalid parameter type`,
		},
		{
			name: "invalid backoffRetry.max returns error",
			config: config.Config{
				"apiKey":             "api-key",
				"model":              "command",
				"modelVersion":       "command",
				"backoffRetry.count": "1",
				"backoffRetry.max":   "not-a-duration",
			},
			wantErr: `failed to parse configuration: config invalid: error validating "backoffRetry.max": "not-a-duration" value is not a duration: invalid parameter type`,
		},
		{
			name: "invalid backoffRetry.factor returns error",
			config: config.Config{
				"apiKey":              "api-key",
				"model":               "command",
				"modelVersion":        "command",
				"backoffRetry.count":  "1",
				"backoffRetry.factor": "not-a-number",
			},
			wantErr: `failed to parse configuration: config invalid: error validating "backoffRetry.factor": "not-a-number" value is not a float: invalid parameter type`,
		},
		{
			name: "model version does not belong to embed model returns error",
			config: config.Config{
				"apiKey":       "api-key",
				"model":        "embed",
				"modelVersion": "command",
			},
			wantErr: `error validating configuration: modelVersion does not belong to provided model`,
		},
		{
			name: "embed model without embedConfig returns error",
			config: config.Config{
				"apiKey":       "api-key",
				"model":        "embed",
				"modelVersion": "embed-v3",
			},
			wantErr: `error validating configuration: embedConfig is required when model is 'embed'`,
		},
		{
			name: "embed model with invalid inputType returns error",
			config: config.Config{
				"apiKey":                "api-key",
				"model":                 "embed",
				"modelVersion":          "embed-v3",
				"embedConfig.inputType": "invalid",
			},
			wantErr: `error validating configuration: invalid or missing inputType for v3 models: invalid`,
		},
		{
			name: "embed model with empty embeddingTypes returns error",
			config: config.Config{
				"apiKey":                "api-key",
				"model":                 "embed",
				"modelVersion":          "embed-v3",
				"embedConfig.inputType": "search_document",
			},
			wantErr: `error validating configuration: atleast one embeddingType must be provided`,
		},
		{
			name: "embed model with invalid embeddingType returns error",
			config: config.Config{
				"apiKey":                     "api-key",
				"model":                      "embed",
				"modelVersion":               "embed-v3",
				"embedConfig.inputType":      "search_document",
				"embedConfig.embeddingTypes": "invalid",
			},
			wantErr: `error validating configuration: invalid embeddingType: invalid`,
		},
		{
			name: "valid embed model configuration",
			config: config.Config{
				"apiKey":                     "api-key",
				"model":                      "embed",
				"modelVersion":               "embed-v3",
				"embedConfig.inputType":      "search_document",
				"embedConfig.embeddingTypes": "float,int8",
			},
			wantErr: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			p := NewProcessor()
			err := p.Configure(context.Background(), tc.config)
			if tc.wantErr == "" {
				is.NoErr(err)
			} else {
				is.True(err != nil)
				is.Equal(tc.wantErr, err.Error())
			}
		})
	}
}
