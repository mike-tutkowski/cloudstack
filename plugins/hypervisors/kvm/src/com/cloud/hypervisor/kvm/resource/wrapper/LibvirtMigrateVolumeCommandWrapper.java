//
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
//

package com.cloud.hypervisor.kvm.resource.wrapper;

import com.cloud.agent.api.storage.MigrateVolumeAnswer;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.hypervisor.kvm.storage.KVMPhysicalDisk;
import com.cloud.hypervisor.kvm.storage.KVMStoragePool;
import org.apache.cloudstack.storage.to.PrimaryDataStoreTO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.cloudstack.utils.qemu.QemuImg;
import org.apache.log4j.Logger;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.storage.MigrateVolumeCommand;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.hypervisor.kvm.storage.KVMStoragePoolManager;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;

import java.util.Map;

@ResourceWrapper(handles =  MigrateVolumeCommand.class)
public final class LibvirtMigrateVolumeCommandWrapper extends CommandWrapper<MigrateVolumeCommand, Answer, LibvirtComputingResource> {
    private static final Logger s_logger = Logger.getLogger(LibvirtMigrateVolumeCommandWrapper.class);

    @Override
    public Answer execute(final MigrateVolumeCommand command, final LibvirtComputingResource libvirtComputingResource) {
        KVMStoragePoolManager storagePoolManager = libvirtComputingResource.getStoragePoolMgr();

        VolumeObjectTO destVolumeObjectTO = (VolumeObjectTO)command.getDestData();
        PrimaryDataStoreTO destPrimaryDataStore = (PrimaryDataStoreTO)destVolumeObjectTO.getDataStore();

        Map<String, String> details = command.getDetails();

        String path = details.get(DiskTO.IQN);

        try {
            VolumeObjectTO srcVolumeObjectTO = (VolumeObjectTO)command.getSrcData();
            PrimaryDataStoreTO srcPrimaryDataStore = (PrimaryDataStoreTO)srcVolumeObjectTO.getDataStore();

            final KVMPhysicalDisk sourceVolume = storagePoolManager.getPhysicalDisk(srcPrimaryDataStore.getPoolType(), srcPrimaryDataStore.getUuid(),
                    srcVolumeObjectTO.getPath());

            sourceVolume.setFormat(QemuImg.PhysicalDiskFormat.valueOf(srcVolumeObjectTO.getFormat().toString()));

            KVMStoragePool destPrimaryStorage = storagePoolManager.getStoragePool(destPrimaryDataStore.getPoolType(), destPrimaryDataStore.getUuid());

            storagePoolManager.connectPhysicalDisk(destPrimaryDataStore.getPoolType(), destPrimaryDataStore.getUuid(), path, details);

            storagePoolManager.copyPhysicalDisk(sourceVolume, path, destPrimaryStorage, command.getWaitInMillSeconds());

            storagePoolManager.disconnectPhysicalDisk(destPrimaryDataStore.getPoolType(), destPrimaryDataStore.getUuid(), path);
        }
        catch (Exception ex) {
            try {
                storagePoolManager.disconnectPhysicalDisk(destPrimaryDataStore.getPoolType(), destPrimaryDataStore.getUuid(), path);
            }
            finally {
                s_logger.warn("Unable to disconnect from the device.");
            }

            return new MigrateVolumeAnswer(command, false, ex.getMessage(), null);
        }

        return new MigrateVolumeAnswer(command, true, null, null);
    }
}
