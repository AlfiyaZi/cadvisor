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

// Handler for "mesos" containers.
package mesos

import (
	"fmt"
	"path"

	"github.com/google/cadvisor/container"
	"github.com/google/cadvisor/container/common"
	containerlibcontainer "github.com/google/cadvisor/container/libcontainer"
	"github.com/google/cadvisor/fs"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/machine"

	"github.com/golang/glog"
	"github.com/opencontainers/runc/libcontainer/cgroups"
	cgroupfs "github.com/opencontainers/runc/libcontainer/cgroups/fs"
	libcontainerconfigs "github.com/opencontainers/runc/libcontainer/configs"
)

type mesosContainerHandler struct {
	// Name of the container for this handler.
	name               string
	cgroupSubsystems   *containerlibcontainer.CgroupSubsystems
	machineInfoFactory info.MachineInfoFactory

	// Absolute path to the cgroup hierarchies of this container.
	// (e.g.: "cpu" -> "/sys/fs/cgroup/cpu/test")
	cgroupPaths map[string]string

	// Manager of this container's cgroups.
	cgroupManager cgroups.Manager

	storageDriver    storageDriver
	fsInfo           fs.FsInfo
	rootfsStorageDir string

	// Metrics to be ignored.
	ignoreMetrics container.MetricSet

	pid int

	labels map[string]string

	libcontainerHandler *containerlibcontainer.Handler

	// Filesystem handler.
	fsHandler common.FsHandler
}

func isRootCgroup(name string) bool {
	return name == "/"
}

func newMesosContainerHandler(
	name string,
	cgroupSubsystems *containerlibcontainer.CgroupSubsystems,
	machineInfoFactory info.MachineInfoFactory,
	storageDriver storageDriver,
	fsInfo fs.FsInfo,
	ignoreMetrics container.MetricSet,
	inHostNamespace bool,
	client *mesosSlaveClient,
) (container.ContainerHandler, error) {
	cgroupPaths := common.MakeCgroupPaths(cgroupSubsystems.MountPoints, name)
	for key, val := range cgroupSubsystems.MountPoints {
		cgroupPaths[key] = path.Join(val, name)
	}

	// Generate the equivalent cgroup manager for this container.
	cgroupManager := &cgroupfs.Manager{
		Cgroups: &libcontainerconfigs.Cgroup{
			Name: name,
		},
		Paths: cgroupPaths,
	}

	rootFs := "/"
	storageDir := "/var/lib/mesos"
	if !inHostNamespace {
		rootFs = "/rootfs"
		storageDir = path.Join(rootFs, storageDir)
	}

	id := ContainerNameToMesosId(name)

	cinfo, _ := client.ContainerInfo(id)
	labels := cinfo.labels
	pid := cinfo.cntr.Status.ExecutorPID

	libcontainerHandler := containerlibcontainer.NewHandler(cgroupManager, rootFs, pid, ignoreMetrics)

	rootfsStorageDir := "/var/lib/mesos/provisioner/containers/"
	switch storageDriver {
	case overlayStorageDriver:
		rootfsStorageDir = path.Join(rootfsStorageDir, id, "backends/overlay/rootfses", "abc")
	}

	handler := &mesosContainerHandler{
		name:                name,
		cgroupSubsystems:    cgroupSubsystems,
		machineInfoFactory:  machineInfoFactory,
		cgroupPaths:         cgroupPaths,
		cgroupManager:       cgroupManager,
		storageDriver:       storageDriver,
		fsInfo:              fsInfo,
		rootfsStorageDir:    rootfsStorageDir,
		ignoreMetrics:       ignoreMetrics,
		pid:                 pid,
		labels:              labels,
		libcontainerHandler: libcontainerHandler,
	}

	// we optionally collect disk usage metrics
	/*if !ignoreMetrics.Has(container.DiskUsageMetrics) {
		otherStorageDir := ""
		//otherStorageDir := "/var/lib/mesos/slaves/a6d7bf7c-024f-43be-97eb-26bd02520691-S0/frameworks/dc5f0139-f0fb-428b-8166-099b1342a75d-0009/executors/cluster-test/runs/a0e30742-0ad9-49ae-9b1b-d7de2b5f8b09"
		handler.fsHandler = common.NewFsHandler(common.DefaultPeriod, rootfsStorageDir, otherStorageDir, fsInfo)
	}*/

	return handler, nil
}

func (self *mesosContainerHandler) ContainerReference() (info.ContainerReference, error) {
	// We only know the container by its one name.
	return info.ContainerReference{
		Name: self.name,
	}, nil
}

func (self *mesosContainerHandler) GetRootNetworkDevices() ([]info.NetInfo, error) {
	nd := []info.NetInfo{}
	if isRootCgroup(self.name) {
		mi, err := self.machineInfoFactory.GetMachineInfo()
		if err != nil {
			return nd, err
		}
		return mi.NetworkDevices, nil
	}
	return nd, nil
}

// Nothing to start up.
func (self *mesosContainerHandler) Start() {
	if self.fsHandler != nil {
		self.fsHandler.Start()
	}
}

// Nothing to clean up.
func (self *mesosContainerHandler) Cleanup() {
	if self.fsHandler != nil {
		self.fsHandler.Stop()
	}
}

func (self *mesosContainerHandler) GetSpec() (info.ContainerSpec, error) {
	const hasNetwork = false
	hasFilesystem := isRootCgroup(self.name)
	spec, err := common.GetSpec(self.cgroupPaths, self.machineInfoFactory, hasNetwork, hasFilesystem)
	if err != nil {
		return spec, err
	}

	if isRootCgroup(self.name) {
		// Check physical network devices for root container.
		nd, err := self.GetRootNetworkDevices()
		if err != nil {
			return spec, err
		}
		spec.HasNetwork = spec.HasNetwork || len(nd) != 0

		// Get memory and swap limits of the running machine
		memLimit, err := machine.GetMachineMemoryCapacity()
		if err != nil {
			glog.Warningf("failed to obtain memory limit for machine container")
			spec.HasMemory = false
		} else {
			spec.Memory.Limit = uint64(memLimit)
			// Spec is marked to have memory only if the memory limit is set
			spec.HasMemory = true
		}

		swapLimit, err := machine.GetMachineSwapCapacity()
		if err != nil {
			glog.Warningf("failed to obtain swap limit for machine container")
		} else {
			spec.Memory.SwapLimit = uint64(swapLimit)
		}
	}

	spec.Labels = self.labels

	return spec, nil
}

func (self *mesosContainerHandler) getFsStats(stats *info.ContainerStats) error {

	mi, err := self.machineInfoFactory.GetMachineInfo()
	if err != nil {
		return err
	}

	if !self.ignoreMetrics.Has(container.DiskIOMetrics) {
		common.AssignDeviceNamesToDiskStats((*common.MachineInfoNamer)(mi), &stats.DiskIo)
	}

	if self.ignoreMetrics.Has(container.DiskUsageMetrics) {
		return nil
	}

	var device string

	switch self.storageDriver {
	case devicemapperStorageDriver:
		// Device has to be the pool name to correlate with the device name as
		// set in the machine info filesystems.
		device = ""
	case aufsStorageDriver, overlayStorageDriver, overlay2StorageDriver:
		deviceInfo, err := self.fsInfo.GetDirFsDevice(self.rootfsStorageDir)
		if err != nil {
			return fmt.Errorf("unable to determine device info for dir: %v: %v", self.rootfsStorageDir, err)
		}
		device = deviceInfo.Device
	default:
		return nil
	}

	var (
		limit  uint64
		fsType string
	)

	// Docker does not impose any filesystem limits for containers. So use capacity as limit.
	for _, fs := range mi.Filesystems {
		if fs.Device == device {
			limit = fs.Capacity
			fsType = fs.Type
			break
		}
	}

	fsStat := info.FsStats{Device: device, Type: fsType, Limit: limit}
	usage := self.fsHandler.Usage()
	fsStat.BaseUsage = usage.BaseUsageBytes
	fsStat.Usage = usage.TotalUsageBytes
	fsStat.Inodes = usage.InodeUsage

	stats.Filesystem = append(stats.Filesystem, fsStat)

	return nil
}

func (self *mesosContainerHandler) GetStats() (*info.ContainerStats, error) {
	stats, err := self.libcontainerHandler.GetStats()
	if err != nil {
		return stats, err
	}

	// Get filesystem stats.
	/*err = self.getFsStats(stats)
	if err != nil {
		return stats, err
	}*/

	return stats, nil
}

func (self *mesosContainerHandler) GetCgroupPath(resource string) (string, error) {
	path, ok := self.cgroupPaths[resource]
	if !ok {
		return "", fmt.Errorf("could not find path for resource %q for container %q\n", resource, self.name)
	}
	return path, nil
}

func (self *mesosContainerHandler) GetContainerLabels() map[string]string {
	return self.labels
}

func (self *mesosContainerHandler) GetContainerIPAddress() string {
	// the IP address for the mesos container corresponds to the system ip address.
	return "127.0.0.1"
}

func (self *mesosContainerHandler) ListContainers(listType container.ListType) ([]info.ContainerReference, error) {
	return common.ListContainers(self.name, self.cgroupPaths, listType)
}

func (self *mesosContainerHandler) ListProcesses(listType container.ListType) ([]int, error) {
	return self.libcontainerHandler.GetProcesses()
}

func (self *mesosContainerHandler) Exists() bool {
	return common.CgroupExists(self.cgroupPaths)
}

func (self *mesosContainerHandler) Type() container.ContainerType {
	return container.ContainerTypeMesos
}
