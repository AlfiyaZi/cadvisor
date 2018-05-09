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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"crypto/tls"
	"crypto/x509"
	"github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const (
	slaveURL = "http://localhost:5051"
	loginURL = "https://leader.mesos/acs/api/v1/auth/login"
	timeout  = 10 * time.Second
)

var (
	mesosClientOnce sync.Once
	mesosClient     *mesosSlaveClient
)

type mesosSlaveClient struct {
	http.Client
	url  string
	auth authInfo
}

type (
	State struct {
		Attributes map[string]json.RawMessage `json:"attributes"`
		Frameworks []Framework                `json:"frameworks"`
	}
	Framework struct {
		Name      string     `json:"name"`
		ID        string     `json:"ID"`
		Executors []Executor `json:"executors"`
	}
	Executor struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Container string `json:"container"`
		Source    string `json:"source"`
		Tasks     []task `json:"tasks"`
	}
)

// Framework finds a framework with the given id and returns nil if not found. Note that
// this is different from the framework name.
func (s State) Framework(id string) *Framework {
	for _, fw := range s.Frameworks {
		if fw.ID == id {
			return &fw
		}
	}
	return nil
}

// Executor finds an executor with the given ID and returns nil if not found. Note that
// this is different from the Executor name.
func (fw Framework) Executor(id string) *Executor {
	for _, exec := range fw.Executors {
		if exec.ID == id {
			return &exec
		}
	}
	return nil
}

//type ranges [][2]uint64

type (
	resources struct {
		Cpus          float64 `json:"cpus"`
		Disk          float64 `json:"disk"`
		Mem           float64 `json:"mem"`
		Ports         string  `json:"ports"`
		CpusRevocable float64 `json:"cpus_revocable"`
	}

	task struct {
		Name        string    `json:"name"`
		ID          string    `json:"id"`
		ExecutorID  string    `json:"executor_id"`
		FrameworkID string    `json:"framework_id"`
		SlaveID     string    `json:"slave_id"`
		State       string    `json:"state"`
		Labels      []label   `json:"labels"`
		Resources   resources `json:"resources"`
		Statuses    []status  `json:"statuses"`
	}

	label struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	status struct {
		State     string  `json:"state"`
		Timestamp float64 `json:"timestamp"`
	}

	tokenResponse struct {
		Token string `json:"token"`
	}

	tokenRequest struct {
		UID   string `json:"uid"`
		Token string `json:"token"`
	}

	mesosSecret struct {
		LoginEndpoint string `json:"login_endpoint"`
		PrivateKey    string `json:"private_key"`
		Scheme        string `json:"scheme"`
		UID           string `json:"uid"`
	}

	mesosContainer struct {
		ContainerID  string `json:"container_id"`
		ExecutorID   string `json:"executor_id"`
		ExecutorName string `json:"executor_name"`
		FrameworkID  string `json:"framework_id"`
		Source       string `json:"source"`
		Status       Status `json:"status"`
	}
)

type containerInfo struct {
	cntr   mesosContainer
	labels map[string]string
}

// ContainerID uniquely identifies a container.
//
// This sometimes, but not always, can be used to infer its cgroup hierarchy inner path.
type ContainerID struct {
	Value string `json:"value"`
}

// Status describes metadata about a container.
type Status struct {
	ContainerID ContainerID `json:"container_id"`
	ExecutorPID int         `json:"executor_pid"`
}

var (
	errNotFoundInMap = errors.New("couldn't find key in map")
)

type authInfo struct {
	username      string
	password      string
	loginURL      string
	token         string
	tokenExpire   int64
	signingKey    []byte
	strictMode    bool
	privateKey    string
	skipSSLVerify bool
}

func signingToken(self *mesosSlaveClient) string {
	signKey, err := jwt.ParseRSAPrivateKeyFromPEM(self.auth.signingKey)
	if err != nil {
		log.Printf("Error parsing privateKey: %s", err)
	}

	expireToken := time.Now().Add(time.Hour * 1).Unix()
	self.auth.tokenExpire = expireToken

	// Create the token
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"uid": self.auth.username,
		"exp": expireToken,
	})
	// Sign and get the complete encoded token as a string
	tokenString, err := token.SignedString(signKey)
	if err != nil {
		log.Printf("Error creating login token: %s", err)
		return ""
	}
	return tokenString
}

func authToken(self *mesosSlaveClient) string {
	currentTime := time.Now().Unix()
	if currentTime > self.auth.tokenExpire {
		url := self.auth.loginURL
		signingToken := signingToken(self)
		body, err := json.Marshal(&tokenRequest{UID: self.auth.username, Token: signingToken})
		if err != nil {
			log.Printf("Error creating JSON request: %s", err)
			return ""
		}
		buffer := bytes.NewBuffer(body)
		req, err := http.NewRequest("POST", url, buffer)
		if err != nil {
			log.Printf("Error creating HTTP request to %s: %s", url, err)
			return ""
		}
		req.Header.Add("Content-Type", "application/json")
		res, err := self.Do(req)
		if err != nil {
			log.Printf("Error fetching %s: %s", url, err)
			return ""
		}
		defer res.Body.Close()

		var token tokenResponse
		if err := json.NewDecoder(res.Body).Decode(&token); err != nil {
			log.Printf("Error decoding response body from %s: %s", url, err)
			return ""
		}

		self.auth.token = fmt.Sprintf("token=%s", token.Token)
	}
	return self.auth.token
}

func (self *mesosSlaveClient) fetchAndDecode(endpoint string, target interface{}) bool {
	url := strings.TrimSuffix(self.url, "/") + endpoint
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("Error creating HTTP request to %s: %s", url, err)
		return false
	}
	if self.auth.username != "" && self.auth.password != "" {
		req.SetBasicAuth(self.auth.username, self.auth.password)
	}
	if self.auth.strictMode {
		req.Header.Add("Authorization", authToken(self))
	}
	res, err := self.Do(req)
	if err != nil {
		log.Printf("Error fetching %s: %s", url, err)
		return false
	}
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&target); err != nil {
		log.Printf("Error decoding response body from %s: %s", url, err)
		return false
	}

	return true
}

func fetchLabelsFromExec(exec *Executor, labels map[string]string) error {
	splits := strings.Split(exec.Source, ".")
	labels["instance"] = splits[len(splits)-1]

	if len(exec.Tasks) != 1 {
		return fmt.Errorf("more than one task found for executor: %v", exec)
	}

	t := exec.Tasks[0]
	// hack to identify revocability. No idea how this should work...
	if t.Resources.CpusRevocable > t.Resources.Cpus {
		labels["scheduler_sla"] = "revocable"
	} else {
		labels["scheduler_sla"] = "non_revocable"
	}

	for _, l := range exec.Tasks[0].Labels {
		if l.Key == "org.apache.aurora.metadata.udeploy" {
			labels[l.Key] = l.Value
		}
	}

	return nil
}

func (self *mesosSlaveClient) getLabels(c *mesosContainer, s *State) (map[string]string, error) {
	labels := make(map[string]string)
	fw := s.Framework(c.FrameworkID)
	if fw == nil {
		return labels, errors.New("framework ID not found")
	}
	labels["framework"] = fw.Name

	exec := fw.Executor(c.ExecutorID)
	if exec == nil {
		return labels, errors.New("executor ID not found")
	}

	if err := fetchLabelsFromExec(exec, labels); err != nil {
		return labels, errors.New("error while fetching labels from executor")
	}
	return labels, nil
}

func (self *mesosSlaveClient) getContainers() ([]mesosContainer, error) {
	var cntrs []mesosContainer
	resp := self.fetchAndDecode("/containers", &cntrs)
	if !resp {
		log.Printf("Failed to get mesos containers")
		return cntrs, errors.New("failed to get mesos containers")
	}
	return cntrs, nil
}

func (self *mesosSlaveClient) getContainer(id string) (*mesosContainer, error) {
	cntrs, err := self.getContainers()
	if err != nil {
		return nil, err
	}
	for _, c := range cntrs {
		if c.ContainerID == id {
			return &c, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("can't locate container %s", id))
}

func (self *mesosSlaveClient) ContainerInfo(id string) (*containerInfo, error) {
	// Get all containers
	c, err := self.getContainer(id)
	cinfo := containerInfo{}
	if err != nil {
		return &cinfo, err
	}

	// Get agent state which contains all containers labels
	var s State
	self.fetchAndDecode("/state", &s)

	// Get labels of the preferred container
	l, err := self.getLabels(c, &s)
	if err != nil {
		return &cinfo, err
	}

	cinfo.cntr = *c
	cinfo.labels = l

	return &cinfo, nil
}

func Client() *mesosSlaveClient {
	mesosClientOnce.Do(func() {
		auth := authInfo{
			strictMode:    false,
			skipSSLVerify: false,
			loginURL:      loginURL,
		}

		auth.privateKey = os.Getenv("MESOS_EXPORTER_PRIVATE_KEY")
		auth.username = os.Getenv("MESOS_EXPORTER_USERNAME")
		auth.password = os.Getenv("MESOS_EXPORTER_PASSWORD")

		// Start Client
		var certPool *x509.CertPool
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: certPool, InsecureSkipVerify: auth.skipSSLVerify},
		}

		// HTTP Redirects are authenticated by Go (>=1.8), when redirecting to an identical domain or a subdomain.
		// -> Hijack redirect authentication, since hostnames rarely follow this logic.
		var redirectFunc func(req *http.Request, via []*http.Request) error
		if auth.username != "" && auth.password != "" {
			// Auth information is only available in the current context -> use lambda function
			redirectFunc = func(req *http.Request, via []*http.Request) error {
				req.SetBasicAuth(auth.username, auth.password)
				return nil
			}
		}

		mesosClient = &mesosSlaveClient{
			http.Client{Timeout: timeout, Transport: transport, CheckRedirect: redirectFunc},
			//		http.Client{Timeout: timeout},
			slaveURL,
			auth,
		}

		if auth.strictMode {
			mesosClient.auth.signingKey = parsePrivateKey(mesosClient)
		}
	})
	return mesosClient
}

func parsePrivateKey(self *mesosSlaveClient) []byte {
	if _, err := os.Stat(self.auth.privateKey); os.IsNotExist(err) {
		buffer := bytes.NewBuffer([]byte(self.auth.privateKey))
		var key mesosSecret
		if err := json.NewDecoder(buffer).Decode(&key); err != nil {
			log.Printf("Error decoding prviate key %s: %s", key, err)
			return []byte{}
		}
		self.auth.username = key.UID
		self.auth.loginURL = key.LoginEndpoint
		return []byte(key.PrivateKey)
	}
	absPath, _ := filepath.Abs(self.auth.privateKey)
	key, err := ioutil.ReadFile(absPath)
	if err != nil {
		log.Printf("Error reading private key %s: %s", absPath, err)
		return []byte{}
	}
	return key
}
