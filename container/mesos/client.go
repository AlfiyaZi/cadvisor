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
	"net/http"
	"strings"
	"time"

	"sync"
)

const (
	slaveURL               = "http://localhost:5051"
	containersHTTPEndpoint = "/containers"
	slaveStateHTTPEndpoint = "/state"
	timeout                = 10 * time.Second
)

var (
	mesosClientOnce sync.Once
	mesosClient     *client
)

type client struct {
	hc  *http.Client
	url string
}

type mesosSlaveClient interface {
	ContainerInfo(id string) (*containerInfo, error)
}

type containerInfo struct {
	cntr   mesosContainer
	labels map[string]string
}

// Client is an interface to query mesos slave http endpoints
func Client() (mesosSlaveClient, error) {
	mesosClientOnce.Do(func() {
		// Start Client
		tr := new(http.Transport)
		mesosClient = &client{
			hc:  &http.Client{Timeout: timeout, Transport: tr},
			url: slaveURL,
		}
	})
	return mesosClient, nil
}

// ContainerInfo returns the container information of the given container id
func (self *client) ContainerInfo(id string) (*containerInfo, error) {
	c, err := self.getContainer(id)
	if err != nil {
		return nil, err
	}

	// Get labels of the container
	l, err := self.getLabels(c)
	if err != nil {
		return nil, err
	}

	return &containerInfo{
		cntr:   *c,
		labels: l,
	}, nil
}

func (self *client) getContainer(id string) (*mesosContainer, error) {
	// Get all containers
	cntrs, err := self.getContainers()
	if err != nil {
		return nil, err
	}

	// Check if there is a container with given id and return the container
	for _, c := range cntrs {
		if c.ContainerID == id {
			return &c, nil
		}
	}
	return nil, fmt.Errorf("can't locate container %s", id)
}

func (self *client) getContainers() ([]mesosContainer, error) {
	var cntrs []mesosContainer
	err := self.fetchAndDecode(containersHTTPEndpoint, &cntrs)
	if err != nil {
		return cntrs, fmt.Errorf("failed to get mesos containers")
	}
	return cntrs, nil
}

func (self *client) getLabels(c *mesosContainer) (map[string]string, error) {
	labels := make(map[string]string)

	// Get mesos slave state which contains all containers labels
	var s state
	err := self.fetchAndDecode(slaveStateHTTPEndpoint, &s)
	if err != nil {
		return nil, err
	}

	// Look for the framework which launched the container.
	fw, err := s.Framework(c.FrameworkID)
	if err != nil {
		return nil, fmt.Errorf("framework ID not found")
	}
	labels["framework"] = fw.Name

	// Get the executor info of the container which contains all the task info.
	exec, err := s.Executor(fw, c.ExecutorID)
	if err != nil {
		return nil, fmt.Errorf("executor ID not found")
	}

	labels, err = fetchLabelsFromExecutor(exec)
	if err != nil {
		return nil, fmt.Errorf("error while fetching labels from executor")
	}
	return labels, nil
}

func (self *client) fetchAndDecode(endpoint string, target interface{}) error {
	url := strings.TrimSuffix(self.url, "/") + endpoint
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP request to %s: %s", url, err)
	}

	res, err := self.hc.Do(req)
	if err != nil {
		return fmt.Errorf("error fetching %s: %s", url, err)
	}
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&target); err != nil {
		return fmt.Errorf("error decoding response body from %s: %s", url, err)
	}

	return nil
}
