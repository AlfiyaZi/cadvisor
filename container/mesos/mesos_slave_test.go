// Copyright 2018 Google Inc. All Rights Reserved.
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

package mesos

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFetchLabelsFromExecutor(t *testing.T) {
	type testCase struct {
		executor       *executor
		expectedError  error
		expectedLabels map[string]string
	}
	for _, ts := range []testCase{
		{
			&executor{
				Name: "executor1",
			},
			fmt.Errorf("number of tasks for executor: executor1 is not one"),
			nil,
		},
		{
			&executor{
				Name: "executor1",
				Tasks: []task{
					{
						Resources: resources{Cpus: 2},
						Labels: []label{
							{
								Key:   "key1",
								Value: "value1",
							},
						},
					},
				},
				Source: "source1",
			},
			nil,
			map[string]string{
				"source":        "source1",
				"scheduler_sla": "non_revocable",
				"key1":          "value1",
			},
		},
	} {

		actualLabels, err := fetchLabelsFromExecutor(ts.executor)
		if ts.expectedError == nil {
			assert.Nil(t, err)
		} else {
			assert.Equal(t, ts.expectedError.Error(), err.Error())
		}
		assert.Equal(t, ts.expectedLabels, actualLabels)
	}
}
