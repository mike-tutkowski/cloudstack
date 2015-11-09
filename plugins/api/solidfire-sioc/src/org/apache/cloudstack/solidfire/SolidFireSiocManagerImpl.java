// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.cloudstack.solidfire;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.cloud.dc.DataCenterVO;
import com.cloud.dc.dao.DataCenterDao;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.hypervisor.vmware.VmwareDatacenterVO;
import com.cloud.hypervisor.vmware.VmwareDatacenterZoneMapVO;
import com.cloud.hypervisor.vmware.dao.VmwareDatacenterDao;
import com.cloud.hypervisor.vmware.dao.VmwareDatacenterZoneMapDao;
import com.cloud.storage.DiskOfferingVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.utils.db.GlobalLock;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;

import com.vmware.vim25.SharesInfo;
import com.vmware.vim25.StorageIOAllocationInfo;
import com.vmware.vim25.VirtualDevice;
import com.vmware.vim25.VirtualDeviceConfigSpec;
import com.vmware.vim25.VirtualDeviceConfigSpecOperation;
import com.vmware.vim25.VirtualDisk;
import com.vmware.vim25.VirtualIDEController;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.VirtualMachineConfigSpec;
import com.vmware.vim25.VirtualSCSIController;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;

@Component
public class SolidFireSiocManagerImpl implements SolidFireSiocManager {
    private static final Logger s_logger = Logger.getLogger(SolidFireSiocManagerImpl.class);
    private static final int s_lockTimeInSeconds = 3;

    @Inject private DataCenterDao _zoneDao;
    @Inject private DiskOfferingDao _diskOfferingDao;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private StoragePoolDetailsDao _storagePoolDetailsDao;
    @Inject private VMInstanceDao _vmInstanceDao;
    @Inject private VmwareDatacenterDao _vmwareDcDao;
    @Inject private VmwareDatacenterZoneMapDao _vmwareDcZoneMapDao;
    @Inject private VolumeDao _volumeDao;

    @Override
    public void updateSiocInfo(long zoneId, String storageTag) throws Exception {
        s_logger.info("'SolidFireSiocManagerImpl.updateSiocInfo(long, String)' method invoked");

        DataCenterVO zone = _zoneDao.findById(zoneId);

        if (zone == null) {
            throw new Exception("No zone could be located for the following zone ID: " + zoneId + ".");
        }

        if (storageTag == null || storageTag.trim().length() == 0) {
            throw new Exception("No storage tag was provided.");
        }

        List<StoragePoolDetailVO> details = _storagePoolDetailsDao.listAll();

        if (details == null || details.size() == 0) {
            throw new Exception("There are no storage tags in the database.");
        }

        String lockName = zone.getUuid() + "-" + storageTag;
        GlobalLock lock = GlobalLock.getInternLock(lockName);

        if (!lock.lock(s_lockTimeInSeconds)) {
            throw new Exception("Busy: The system is already processing this request.");
        }

        ServiceInstance serviceInstance = null;

        try {
            serviceInstance = getServiceInstance(zoneId);

            List<Task> allTasks = new ArrayList<>();

            for (StoragePoolDetailVO detail : details) {
                if (storageTag.equalsIgnoreCase(detail.getName()) && Boolean.TRUE.toString().equalsIgnoreCase(detail.getValue())) {
                    long storagePoolId = detail.getResourceId();
                    StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

                    if (storagePool != null && storagePool.getDataCenterId() == zoneId &&
                            storagePool.getHypervisor().equals(HypervisorType.VMware)) {
                        List<VolumeVO> volumes = _volumeDao.findByPoolId(storagePoolId, null);

                        if (volumes != null && volumes.size() > 0) {
                            Set<Long> instanceIds = new HashSet<>();

                            for (VolumeVO volume : volumes) {
                                Long instanceId = volume.getInstanceId();

                                if (instanceId != null) {
                                    instanceIds.add(instanceId);
                                }
                            }

                            for (Long instanceId : instanceIds) {
                                List<Task> tasks = updateSiocInfo(instanceId, storagePool, serviceInstance);

                                allTasks.addAll(tasks);
                            }
                        }
                    }
                }
            }

            for (Task task : allTasks) {
                task.waitForTask();
            }
        }
        finally {
            closeServiceInstance(serviceInstance);

            lock.unlock();
            lock.releaseRef();
        }
    }

    private List<Task> updateSiocInfo(Long instanceId, StoragePoolVO storagePool, ServiceInstance serviceInstance) throws Exception {
        List<Task> tasks = new ArrayList<>();

        VMInstanceVO vmInstance = _vmInstanceDao.findById(instanceId);

        if (vmInstance == null) {
            s_logger.warn("The VM with ID " + instanceId + " could not be located.");

            return tasks;
        }

        String vmName = vmInstance.getInstanceName();

        VirtualMachine vm = (VirtualMachine)new InventoryNavigator(serviceInstance.getRootFolder()).searchManagedEntity("VirtualMachine", vmName);
        VirtualMachineConfigInfo vmci = vm.getConfig();
        VirtualDevice[] devices = vmci.getHardware().getDevice();

        for (VirtualDevice device : devices) {
            if (device instanceof VirtualDisk) {
                VirtualDisk disk = (VirtualDisk)device;

                VolumeVO volumeVO = getVolumeFromVirtualDisk(vmInstance, storagePool.getId(), devices, disk);

                if (volumeVO.getPoolId() != null && volumeVO.getPoolId() == storagePool.getId()) {
                    boolean diskUpdated = false;

                    SharesInfo sharesInfo = disk.getShares();

                    int currentShares = sharesInfo.getShares();
                    Integer newShares = getNewShares(volumeVO);

                    if (newShares != null && currentShares != newShares) {
                        sharesInfo.setShares(newShares);

                        disk.setShares(sharesInfo);

                        diskUpdated = true;
                    }

                    StorageIOAllocationInfo sioai = disk.getStorageIOAllocation();

                    long currentLimitIops = sioai.getLimit() !=  null ? sioai.getLimit() : Long.MIN_VALUE;
                    Long newLimitIops = getNewLimitIops(volumeVO);

                    if (newLimitIops != null && currentLimitIops != newLimitIops) {
                        sioai.setLimit(newLimitIops);

                        disk.setStorageIOAllocation(sioai);

                        diskUpdated = true;
                    }

                    if (diskUpdated) {
                        VirtualDeviceConfigSpec vdcs = new VirtualDeviceConfigSpec();

                        vdcs.setDevice(disk);
                        vdcs.setOperation(VirtualDeviceConfigSpecOperation.edit);

                        VirtualMachineConfigSpec vmcs = new VirtualMachineConfigSpec();

                        vmcs.setDeviceChange(new VirtualDeviceConfigSpec[] { vdcs });

                        Task task = vm.reconfigVM_Task(vmcs);

                        tasks.add(task);
                    }
                }
            }
        }

        return tasks;
    }

    private VolumeVO getVolumeFromVirtualDisk(VMInstanceVO vmInstance, long storagePoolId, VirtualDevice[] allDevices,
            VirtualDisk disk) throws Exception {
        List<VolumeVO> volumes = _volumeDao.findByInstance(vmInstance.getId());

        final String errMsg = "The VMware virtual disk " + disk.getDiskObjectId() + " could not be mapped to a CloudStack volume.";

        if (volumes == null || volumes.size() == 0) {
            throw new Exception(errMsg + " There were no volumes for the VM with the following ID: " + vmInstance.getId() + ".");
        }

        for (VolumeVO volume : volumes) {
            if (volume.getPoolId() != null && volume.getPoolId() == storagePoolId) {
                String deviceBusName = getDeviceBusName(allDevices, disk);

                if (volume.getChainInfo().contains(deviceBusName)) {
                    return volume;
                }
            }
        }

        throw new Exception(errMsg);
    }

    private String getDeviceBusName(VirtualDevice[] allDevices, VirtualDisk disk) throws Exception {
        for (VirtualDevice device : allDevices) {
            if (device.getKey() == disk.getControllerKey().intValue()) {
                if (device instanceof VirtualIDEController) {
                    return String.format("ide%d:%d", ((VirtualIDEController)device).getBusNumber(), disk.getUnitNumber());
                } else if (device instanceof VirtualSCSIController) {
                    return String.format("scsi%d:%d", ((VirtualSCSIController)device).getBusNumber(), disk.getUnitNumber());
                } else {
                    throw new Exception("The device controller is not supported.");
                }
            }
        }

        throw new Exception("The device controller could not be located.");
    }

    private Integer getNewShares(VolumeVO volumeVO) {
        Long diskOfferingId = volumeVO.getDiskOfferingId();

        if (diskOfferingId == null) {
            return null;
        }

        DiskOfferingVO diskOffering = _diskOfferingDao.findById(diskOfferingId);

        if (diskOffering == null) {
            return null;
        }

        Long minIops = diskOffering.getMinIops();

        if (minIops == null) {
            return null;
        }

        // it's OK to convert from Long to int here (the IOPS top out at 100,000 per volume)
        return minIops.intValue();
    }

    private Long getNewLimitIops(VolumeVO volumeVO) {
        Long diskOfferingId = volumeVO.getDiskOfferingId();

        if (diskOfferingId == null) {
            return null;
        }

        DiskOfferingVO diskOffering = _diskOfferingDao.findById(diskOfferingId);

        if (diskOffering == null) {
            return null;
        }

        Long maxIops = diskOffering.getMaxIops();

        if (maxIops == null) {
            return null;
        }

        return maxIops;
    }

    private ServiceInstance getServiceInstance(long zoneId) throws RemoteException, MalformedURLException {
        VmwareDatacenterZoneMapVO vmwareDcZoneMap = _vmwareDcZoneMapDao.findByZoneId(zoneId);
        Long associatedVmwareDcId = vmwareDcZoneMap.getVmwareDcId();
        VmwareDatacenterVO associatedVmwareDc = _vmwareDcDao.findById(associatedVmwareDcId);

        String host = associatedVmwareDc.getVcenterHost();
        String username = associatedVmwareDc.getUser();
        String password = associatedVmwareDc.getPassword();

        return new ServiceInstance(new URL("https://" + host + "/sdk"), username, password, true);
    }

    private void closeServiceInstance(ServiceInstance serviceInstance) {
        if (serviceInstance != null) {
            serviceInstance.getServerConnection().logout();
        }
    }
}
