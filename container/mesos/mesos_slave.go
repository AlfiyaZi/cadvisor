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
	"encoding/json"
	"fmt"
)

// Mesos slave state json structure
type (
	state struct {
		Attributes map[string]json.RawMessage `json:"attributes"`
		Frameworks []framework                `json:"frameworks"`
	}

	framework struct {
		Name      string     `json:"name"`
		ID        string     `json:"ID"`
		Executors []executor `json:"executors"`
	}

	executor struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Container string `json:"container"`
		Source    string `json:"source"`
		Tasks     []task `json:"tasks"`
	}

	task struct {
		Name        string       `json:"name"`
		ID          string       `json:"id"`
		ExecutorID  string       `json:"executor_id"`
		FrameworkID string       `json:"framework_id"`
		SlaveID     string       `json:"slave_id"`
		State       string       `json:"state"`
		Labels      []label      `json:"labels"`
		Resources   resources    `json:"resources"`
		Statuses    []taskStatus `json:"statuses"`
	}

	resources struct {
		Cpus          float64 `json:"cpus"`
		Disk          float64 `json:"disk"`
		Mem           float64 `json:"mem"`
		Ports         string  `json:"ports"`
		CpusRevocable float64 `json:"cpus_revocable"`
	}

	label struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	taskStatus struct {
		State     string  `json:"state"`
		Timestamp float64 `json:"timestamp"`
	}
)

// Mesos container json structure
type (
	mesosContainer struct {
		ContainerID  string `json:"container_id"`
		ExecutorID   string `json:"executor_id"`
		ExecutorName string `json:"executor_name"`
		FrameworkID  string `json:"framework_id"`
		Source       string `json:"source"`
		Status       status `json:"status"`
	}

	status struct {
		ContainerID containerID `json:"container_id"`
		ExecutorPID int         `json:"executor_pid"`
	}

	containerID struct {
		Value string `json:"value"`
	}
)

// Framework finds a framework with the given id and returns nil if not found. Note that
// this is different from the framework name.
func (s *state) Framework(id string) (*framework, error) {
	for _, fw := range s.Frameworks {
		if fw.ID == id {
			return &fw, nil
		}
	}
	return nil, fmt.Errorf("unable to find framework for mesos container %s", id)
}

// Executor finds an executor with the given ID and returns nil if not found. Note that
// this is different from the Executor name.
func (s *state) Executor(fw *framework, id string) (*executor, error) {
	for _, exec := range fw.Executors {
		if exec.ID == id {
			return &exec, nil
		}
	}
	return nil, fmt.Errorf("unable to find executor for mesos container %s in framework %v", id, fw)
}

func fetchLabelsFromExecutor(exec *executor) (map[string]string, error) {
	labels := make(map[string]string)

	if len(exec.Tasks) != 1 {
		return nil, fmt.Errorf("number of tasks for executor: %v is not one", exec.Name)
	}

	labels["source"] = exec.Source

	t := exec.Tasks[0]

	// Identify revocability. Can be removed once we have a proper label
	if t.Resources.CpusRevocable > t.Resources.Cpus {
		labels["scheduler_sla"] = "revocable"
	} else {
		labels["scheduler_sla"] = "non_revocable"
	}

	for _, l := range exec.Tasks[0].Labels {
		labels[l.Key] = l.Value
	}

	return labels, nil
}
