// Copyright 2014 Google Inc. All Rights Reserved.
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
	"path"
	"regexp"
	"strings"

	"github.com/golang/glog"
	"github.com/google/cadvisor/container"
	"github.com/google/cadvisor/container/libcontainer"
	"github.com/google/cadvisor/fs"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/manager/watcher"

)

// The namespace under which mesos aliases are unique.
const MesosNamespace = "mesos"

// Regexp that identifies mesos cgroups, containers started with
// --cgroup-parent have another prefix than 'mesos'
var mesosCgroupRegexp = regexp.MustCompile(`([a-z0-9]{64})`)

type storageDriver string

const (
	devicemapperStorageDriver storageDriver = "devicemapper"
	aufsStorageDriver         storageDriver = "aufs"
	overlayStorageDriver      storageDriver = "overlay"
	overlay2StorageDriver     storageDriver = "overlay2"
	zfsStorageDriver          storageDriver = "zfs"
)


type mesosFactory struct {
	// Factory for machine information.
	machineInfoFactory info.MachineInfoFactory

	// Information about the cgroup subsystems.
	cgroupSubsystems *libcontainer.CgroupSubsystems

	storageDriver storageDriver

	// Information about mounted filesystems.
	fsInfo fs.FsInfo

	// List of metrics to be ignored.
	ignoreMetrics map[container.MetricKind]struct{}

	//Mesos client
	client *mesosSlaveClient
}

func (self *mesosFactory) String() string {
	return MesosNamespace
}

func (self *mesosFactory) NewContainerHandler(name string, inHostNamespace bool) (container.ContainerHandler, error) {
	client := Client()

	return newMesosContainerHandler(
		name,
		self.cgroupSubsystems,
		self.machineInfoFactory,
		self.storageDriver,
		self.fsInfo,
		self.ignoreMetrics,
		inHostNamespace,
		client,
	)
}

// Returns the Mesos ID from the full container name.
func ContainerNameToMesosId(name string) string {
	id := path.Base(name)

	if matches := mesosCgroupRegexp.FindStringSubmatch(id); matches != nil {
		return matches[1]
	}

	return id
}

// isContainerName returns true if the cgroup with associated name
// corresponds to a mesos container.
func isContainerName(name string) bool {
	// always ignore .mount cgroup even if associated with mesos and delegate to systemd
	if strings.HasSuffix(name, ".mount") {
		return false
	}
	return mesosCgroupRegexp.MatchString(path.Base(name))
}

// The mesos factory can handle any container.
func (self *mesosFactory) CanHandleAndAccept(name string) (bool, bool, error) {
	/*if !strings.HasPrefix(path.Base(name), MesosNamespace) {
		return false, false, nil
	}
	// if the container is not associated with CRI-O, we can't handle it or accept it.
	if !isContainerName(name) {
		return false, false, nil
	}*/

	accept := strings.Contains(name, "/mesos/")
	return accept, accept, nil
}

func (self *mesosFactory) DebugInfo() map[string][]string {
	return map[string][]string{}
}

func Register(
	machineInfoFactory info.MachineInfoFactory,
	fsInfo fs.FsInfo,
	ignoreMetrics container.MetricSet,
) error {
	client := Client()
	cgroupSubsystems, err := libcontainer.GetCgroupSubsystems()
	if err != nil {
		return fmt.Errorf("failed to get cgroup subsystems: %v", err)
	}

	//TODO: Decide if we need a watcher

	glog.V(1).Infof("Registering mesos factory")
	factory := &mesosFactory{
		machineInfoFactory: machineInfoFactory,
		cgroupSubsystems:   &cgroupSubsystems,
		storageDriver:      storageDriver(overlayStorageDriver),
		fsInfo:             fsInfo,
		ignoreMetrics:      ignoreMetrics,
		client:				client,
	}
	container.RegisterContainerHandlerFactory(factory, []watcher.ContainerWatchSource{watcher.Raw})
	return nil
}
