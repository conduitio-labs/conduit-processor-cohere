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

	cohere "github.com/cohere-ai/cohere-go/v2"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func (p *Processor) processCommandModel(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	out := make([]sdk.ProcessedRecord, 0, len(records))
	for _, record := range records {
		resp, err := p.client.V2.Chat(
			ctx,
			&cohere.V2ChatRequest{
				Model: p.config.ModelVersion,
				Messages: cohere.ChatMessages{
					{
						Role: "user",
						User: &cohere.UserMessage{Content: &cohere.UserMessageContent{
							String: string(record.Payload.After.Bytes()),
						}},
					},
				},
			},
		)
		if err != nil {
			return append(out, sdk.ErrorRecord{Error: err})
		}

		err = p.setField(&record, p.referenceResolver, resp.String())
		if err != nil {
			return append(out, sdk.ErrorRecord{Error: fmt.Errorf("failed setting response body: %w", err)})
		}

		out = append(out, sdk.SingleRecord(record))
	}
	return out
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
