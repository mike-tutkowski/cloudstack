/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cloudstack.storage.motion;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.inject.Inject;

import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataMotionStrategy;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreCapabilities;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.ObjectInDataStoreStateMachine.Event;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.StrategyPriority;
import org.apache.cloudstack.engine.subsystem.api.storage.TemplateInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService.VolumeApiResult;
import org.apache.cloudstack.framework.async.AsyncCallFuture;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.framework.config.dao.ConfigurationDao;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.cloudstack.storage.command.CopyCmdAnswer;
import org.apache.cloudstack.storage.command.CopyCommand;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.cloud.agent.AgentManager;
import com.cloud.agent.api.storage.MigrateVolumeAnswer;
import com.cloud.agent.api.storage.MigrateVolumeCommand;
import com.cloud.agent.api.to.DataTO;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.agent.api.to.VirtualMachineTO;
import com.cloud.configuration.Config;
import com.cloud.exception.AgentUnavailableException;
import com.cloud.exception.OperationTimedoutException;
import com.cloud.host.Host;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.storage.DataStoreRole;
import com.cloud.storage.Storage.ImageFormat;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.dao.VolumeDetailsDao;
import com.cloud.utils.NumbersUtil;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.VirtualMachine;
import com.cloud.vm.VirtualMachineManager;

import com.google.common.base.Preconditions;

@Component
public class StorageSystemDataMotionStrategy implements DataMotionStrategy {
    private static final Logger LOGGER = Logger.getLogger(StorageSystemDataMotionStrategy.class);
    private static final Random RANDOM = new Random(System.nanoTime());

    @Inject private AgentManager _agentMgr;
    @Inject private ConfigurationDao _configDao;
    @Inject private DataStoreManager dataStoreMgr;
    @Inject private DiskOfferingDao _diskOfferingDao;
    @Inject private HostDao _hostDao;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private SnapshotDao _snapshotDao;
    @Inject private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject private VolumeDataFactory _volumeDataFactory;
    @Inject private VolumeDetailsDao _volumeDetailsDao;
    @Inject private VolumeDao _volumeDao;
    @Inject private VolumeService _volumeService;

    @Override
    public StrategyPriority canHandle(DataObject srcData, DataObject destData) {
        if (srcData instanceof SnapshotInfo) {
            if (canHandle(srcData) || canHandle(destData)) {
                return StrategyPriority.HIGHEST;
            }
        }

        if (srcData instanceof TemplateInfo && destData instanceof VolumeInfo &&
                (srcData.getDataStore().getId() == destData.getDataStore().getId()) &&
                (canHandle(srcData) || canHandle(destData))) {
            // Both source and dest are on the same storage, so just clone them.
            return StrategyPriority.HIGHEST;
        }

        if (srcData instanceof VolumeInfo && destData instanceof VolumeInfo) {
            VolumeInfo srcVolumeInfo = (VolumeInfo)srcData;

            if (isVolumeOnManagedStorage(srcVolumeInfo)) {
                return StrategyPriority.HIGHEST;
            }

            VolumeInfo destVolumeInfo = (VolumeInfo)destData;

            if (isVolumeOnManagedStorage(destVolumeInfo)) {
                return StrategyPriority.HIGHEST;
            }
        }

        return StrategyPriority.CANT_HANDLE;
    }

    private boolean isVolumeOnManagedStorage(VolumeInfo volumeInfo) {
        long storagePooldId = volumeInfo.getDataStore().getId();
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(storagePooldId);

        return storagePoolVO.isManaged();
    }

    private boolean canHandle(DataObject dataObject) {
        Preconditions.checkArgument(dataObject != null, "Passing 'null' to dataObject of canHandle(DataObject) is not supported.");

        DataStore dataStore = dataObject.getDataStore();

        if (dataStore.getRole() == DataStoreRole.Primary) {
            Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

            if (mapCapabilities == null) {
                return false;
            }

            if (dataObject instanceof VolumeInfo || dataObject instanceof  SnapshotInfo) {
                String value = mapCapabilities.get(DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString());
                Boolean supportsStorageSystemSnapshots = Boolean.valueOf(value);

                if (supportsStorageSystemSnapshots) {
                    LOGGER.info("Using 'StorageSystemDataMotionStrategy' (dataObject is a volume or snapshot and the storage system supports snapshots)");

                    return true;
                }
            } else if (dataObject instanceof TemplateInfo) {
                // If the storage system can clone volumes, we can cache templates on it.
                String value = mapCapabilities.get(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_VOLUME.toString());
                Boolean canCloneVolume = Boolean.valueOf(value);

                if (canCloneVolume) {
                    LOGGER.info("Using 'StorageSystemDataMotionStrategy' (dataObject is a template and the storage system can create a volume from a volume)");

                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public StrategyPriority canHandle(Map<VolumeInfo, DataStore> volumeMap, Host srcHost, Host destHost) {
        return StrategyPriority.CANT_HANDLE;
    }

    @Override
    public Void copyAsync(DataObject srcData, DataObject destData, AsyncCompletionCallback<CopyCommandResult> callback) {
        return copyAsync(srcData, destData, null, callback);
    }

    @Override
    public Void copyAsync(DataObject srcData, DataObject destData, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        if (srcData instanceof SnapshotInfo) {
            SnapshotInfo snapshotInfo = (SnapshotInfo)srcData;

            validate(snapshotInfo);

            boolean canHandleSrc = canHandle(srcData);

            if (canHandleSrc && destData instanceof TemplateInfo &&
                    (destData.getDataStore().getRole() == DataStoreRole.Image || destData.getDataStore().getRole() == DataStoreRole.ImageCache)) {
                handleCreateTemplateFromSnapshot(snapshotInfo, (TemplateInfo)destData, callback);

                return null;
            }

            if (destData instanceof VolumeInfo) {
                VolumeInfo volumeInfo = (VolumeInfo)destData;

                boolean canHandleDest = canHandle(destData);

                if (canHandleSrc && canHandleDest) {
                    if (snapshotInfo.getDataStore().getId() == volumeInfo.getDataStore().getId()) {
                        handleCreateVolumeFromSnapshotBothOnStorageSystem(snapshotInfo, volumeInfo, callback);

                        return null;
                    }
                    else {
                        String errMsg = "This operation is not supported (DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT " +
                                "not supported by source or destination storage plug-in). " + getSrcDestDataStoreMsg(srcData, destData);

                        LOGGER.warn(errMsg);

                        invokeCallback(errMsg, callback);

                        throw new UnsupportedOperationException(errMsg);
                    }
                }

                if (canHandleDest) {
                    handleCreateVolumeFromSnapshotOnSecondaryStorage(snapshotInfo, volumeInfo, callback);

                    return null;
                }

                if (canHandleSrc) {
                    String errMsg = "This operation is not supported (DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT " +
                            "not supported by destination storage plug-in). " + getDestDataStoreMsg(destData);

                    LOGGER.warn(errMsg);

                    invokeCallback(errMsg, callback);

                    throw new UnsupportedOperationException(errMsg);
                }
            }
        } else if (srcData instanceof TemplateInfo && destData instanceof VolumeInfo) {
            boolean canHandleSrc = canHandle(srcData);

            if (!canHandleSrc) {
                String errMsg = "This operation is not supported (DataStoreCapabilities.STORAGE_CAN_CREATE_VOLUME_FROM_VOLUME " +
                        "not supported by destination storage plug-in). " + getDestDataStoreMsg(destData);

                LOGGER.warn(errMsg);

                invokeCallback(errMsg, callback);

                throw new UnsupportedOperationException(errMsg);
            }

            handleCreateVolumeFromTemplateBothOnStorageSystem((TemplateInfo)srcData, (VolumeInfo)destData, callback);

            return null;
        } else if (srcData instanceof VolumeInfo && destData instanceof VolumeInfo) {
            VolumeInfo srcVolumeInfo = (VolumeInfo)srcData;

            if (srcVolumeInfo.getState() == Volume.State.Migrating) {
                if (isVolumeOnManagedStorage(srcVolumeInfo)) {
                    String errMsg = "The source volume to migrate is on managed storage. Migration in this case is not yet supported.";

                    LOGGER.warn(errMsg);

                    invokeCallback(errMsg, callback);

                    throw new UnsupportedOperationException(errMsg);
                }

                VolumeInfo destVolumeInfo = (VolumeInfo)destData;

                if (!isVolumeOnManagedStorage(destVolumeInfo)) {
                    String errMsg = "The 'StorageSystemDataMotionStrategy' does not support this migration use case.";

                    LOGGER.warn(errMsg);

                    invokeCallback(errMsg, callback);

                    throw new UnsupportedOperationException(errMsg);
                }

                handleVolumeMigrationFromNonManagedStorageToManagedStorage(srcVolumeInfo, destVolumeInfo, callback);

                return null;
            }
            else if (srcVolumeInfo.getState() == Volume.State.Uploaded &&
                     (srcData.getDataStore().getRole() == DataStoreRole.Image || srcData.getDataStore().getRole() == DataStoreRole.ImageCache) &&
                     destData.getDataStore().getRole() == DataStoreRole.Primary) {
                VolumeInfo destVolumeInfo = (VolumeInfo)destData;

                ImageFormat imageFormat = destVolumeInfo.getFormat();

                if (!ImageFormat.QCOW2.equals(imageFormat)) {
                    String errMsg = "The 'StorageSystemDataMotionStrategy' does not support this upload use case (non KVM).";

                    LOGGER.warn(errMsg);

                    invokeCallback(errMsg, callback);

                    throw new UnsupportedOperationException(errMsg);
                }

                handleCreateVolumeFromVolumeOnSecondaryStorage(srcVolumeInfo, destVolumeInfo, destVolumeInfo.getDataCenterId(),
                        HypervisorType.KVM, callback);

                return null;
            }
        }

        String errMsg = "This operation is not supported.";

        LOGGER.warn(errMsg);

        invokeCallback(errMsg, callback);

        throw new UnsupportedOperationException(errMsg);
    }

    private void invokeCallback(String errMsg, AsyncCompletionCallback<CopyCommandResult> callback) {
        CopyCmdAnswer copyCmdAnswer = new CopyCmdAnswer(errMsg);

        CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

        result.setResult(errMsg);

        callback.complete(result);
    }

    private String getSrcDestDataStoreMsg(DataObject srcData, DataObject destData) {
        Preconditions.checkArgument(srcData != null, "Passing 'null' to srcData of getSrcDestDataStoreMsg(DataObject, DataObject) is not supported.");
        Preconditions.checkArgument(destData != null, "Passing 'null' to destData of getSrcDestDataStoreMsg(DataObject, DataObject) is not supported.");

        return "Source data store = " + srcData.getDataStore().getName() + "; " + "Destination data store = " + destData.getDataStore().getName() + ".";
    }

    private String getSrcDataStoreMsg(DataObject srcData) {
        Preconditions.checkArgument(srcData != null, "Passing 'null' to srcData of getSrcDataStoreMsg(DataObject) is not supported.");

        return "Source data store = " + srcData.getDataStore().getName() + ".";
    }

    private String getDestDataStoreMsg(DataObject destData) {
        Preconditions.checkArgument(destData != null, "Passing 'null' to destData of getDestDataStoreMsg(DataObject) is not supported.");

        return "Destination data store = " + destData.getDataStore().getName() + ".";
    }

    private void validate(SnapshotInfo snapshotInfo) {
        long volumeId = snapshotInfo.getVolumeId();

        VolumeVO volumeVO = _volumeDao.findByIdIncludingRemoved(volumeId);

        if (volumeVO.getFormat() != ImageFormat.QCOW2) {
            throw new CloudRuntimeException("Only the " + ImageFormat.QCOW2.toString() + " image type is currently supported.");
        }
    }

    private void handleVolumeMigrationFromNonManagedStorageToManagedStorage(VolumeInfo srcVolumeInfo, VolumeInfo destVolumeInfo,
                                                                            AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;

        try {
            HypervisorType hypervisorType = srcVolumeInfo.getHypervisorType();

            if (!HypervisorType.KVM.equals(hypervisorType)) {
                throw new CloudRuntimeException("Currently, only the KVM hypervisor type is supported for the migration of a volume " +
                        "from non-managed storage to managed storage.");
            }

            VirtualMachine vm = srcVolumeInfo.getAttachedVM();

            if (vm != null && (vm.getState() != VirtualMachine.State.Stopped && vm.getState() != VirtualMachine.State.Migrating)) {
                throw new CloudRuntimeException("Currently, if a volume to migrate from non-managed storage to managed storage is attached to " +
                        "a VM, the VM must be in the Stopped state.");
            }

            destVolumeInfo.getDataStore().getDriver().createAsync(destVolumeInfo.getDataStore(), destVolumeInfo, null);

            VolumeVO volumeVO = _volumeDao.findById(destVolumeInfo.getId());

            volumeVO.setPath(volumeVO.get_iScsiName());

            _volumeDao.update(volumeVO.getId(), volumeVO);

            destVolumeInfo = _volumeDataFactory.getVolume(destVolumeInfo.getId(), destVolumeInfo.getDataStore());

            long srcStoragePoolId = srcVolumeInfo.getPoolId();
            StoragePoolVO srcStoragePoolVO = _storagePoolDao.findById(srcStoragePoolId);

            HostVO hostVO;

            if (srcStoragePoolVO.getClusterId() != null) {
                hostVO = getHostInCluster(srcStoragePoolVO.getClusterId());
            }
            else {
                hostVO = getHost(destVolumeInfo.getDataCenterId(), hypervisorType);
            }

            // migrate the volume via the hypervisor
            MigrateVolumeAnswer migrateVolumeAnswer = migrateVolume(srcVolumeInfo, destVolumeInfo, hostVO);

            if (migrateVolumeAnswer == null || !migrateVolumeAnswer.getResult()) {
                if (migrateVolumeAnswer != null && !StringUtils.isEmpty(migrateVolumeAnswer.getDetails())) {
                    throw new CloudRuntimeException(migrateVolumeAnswer.getDetails());
                }
                else {
                    throw new CloudRuntimeException("Unable to migrate the volume from non-managed storage to managed storage");
                }
            }
        }
        catch (Exception ex) {
            errMsg = "Migration operation failed in 'StorageSystemDataMotionStrategy.handleVolumeMigrationFromNonManagedStorageToManagedStorage': " +
                    ex.getMessage();

            throw new CloudRuntimeException(errMsg);
        }
        finally {
            CopyCmdAnswer copyCmdAnswer;

            if (errMsg != null) {
                copyCmdAnswer = new CopyCmdAnswer(errMsg);
            }
            else {
                destVolumeInfo = _volumeDataFactory.getVolume(destVolumeInfo.getId(), destVolumeInfo.getDataStore());

                DataTO dataTO = destVolumeInfo.getTO();

                copyCmdAnswer = new CopyCmdAnswer(dataTO);
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
    }

    private void handleCreateTemplateFromSnapshot(SnapshotInfo snapshotInfo, TemplateInfo templateInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;
        CopyCmdAnswer copyCmdAnswer = null;
        boolean usingBackendSnapshot = false;

        try {
            snapshotInfo.processEvent(Event.CopyingRequested);

            HostVO hostVO = getHost(snapshotInfo.getDataCenterId());

            usingBackendSnapshot = usingBackendSnapshotFor(snapshotInfo);

            if (usingBackendSnapshot) {
                createVolumeFromSnapshot(snapshotInfo);
            }

            DataStore srcDataStore = snapshotInfo.getDataStore();

            String value = _configDao.getValue(Config.PrimaryStorageDownloadWait.toString());
            int primaryStorageDownloadWait = NumberUtils.toInt(value, Integer.parseInt(Config.PrimaryStorageDownloadWait.getDefaultValue()));
            CopyCommand copyCommand = new CopyCommand(snapshotInfo.getTO(), templateInfo.getTO(), primaryStorageDownloadWait, VirtualMachineManager.ExecuteInSequence.value());

            try {
                _volumeService.grantAccess(snapshotInfo, hostVO, srcDataStore);

                Map<String, String> srcDetails = getSnapshotDetails(snapshotInfo);

                copyCommand.setOptions(srcDetails);

                copyCmdAnswer = (CopyCmdAnswer)_agentMgr.send(hostVO.getId(), copyCommand);

                if (!copyCmdAnswer.getResult()) {
                    // We were not able to copy. Handle it.
                    errMsg = copyCmdAnswer.getDetails();

                    throw new CloudRuntimeException(errMsg);
                }
            }
            catch (CloudRuntimeException | AgentUnavailableException | OperationTimedoutException ex) {
                String msg = "Failed to create template from snapshot (Snapshot ID = " + snapshotInfo.getId() + ") : ";

                LOGGER.warn(msg, ex);

                throw new CloudRuntimeException(msg + ex.getMessage());
            }
            finally {
                try {
                    _volumeService.revokeAccess(snapshotInfo, hostVO, srcDataStore);
                }
                catch (Exception ex) {
                    LOGGER.warn("Error revoking access to snapshot (Snapshot ID = " + snapshotInfo.getId() + "): " + ex.getMessage(), ex);
                }

                if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
                    if (copyCmdAnswer != null && !StringUtils.isEmpty(copyCmdAnswer.getDetails())) {
                        errMsg = copyCmdAnswer.getDetails();
                    }
                    else {
                        errMsg = "Unable to create template from snapshot";
                    }
                }

                try {
                    if (StringUtils.isEmpty(errMsg)) {
                        snapshotInfo.processEvent(Event.OperationSuccessed);
                    }
                    else {
                        snapshotInfo.processEvent(Event.OperationFailed);
                    }
                }
                catch (Exception ex) {
                    LOGGER.warn("Error processing snapshot event: " + ex.getMessage(), ex);
                }
            }
        }
        catch (Exception ex) {
            errMsg = ex.getMessage();

            throw new CloudRuntimeException(errMsg);
        }
        finally {
            if (usingBackendSnapshot) {
                deleteVolumeFromSnapshot(snapshotInfo);
            }

            if (copyCmdAnswer == null) {
                copyCmdAnswer = new CopyCmdAnswer(errMsg);
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
    }

    private Map<String, String> getVolumeDetails(VolumeInfo volumeInfo) {
        Map<String, String> volumeDetails = new HashMap<>();

        VolumeVO volumeVO = _volumeDao.findById(volumeInfo.getId());

        long storagePoolId = volumeVO.getPoolId();
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(storagePoolId);

        volumeDetails.put(DiskTO.STORAGE_HOST, storagePoolVO.getHostAddress());
        volumeDetails.put(DiskTO.STORAGE_PORT, String.valueOf(storagePoolVO.getPort()));
        volumeDetails.put(DiskTO.IQN, volumeVO.get_iScsiName());

        volumeDetails.put(DiskTO.VOLUME_SIZE, String.valueOf(volumeVO.getSize()));

        return volumeDetails;
    }

    private MigrateVolumeAnswer migrateVolume(VolumeInfo srcVolumeInfo, VolumeInfo destVolumeInfo, HostVO hostVO) {
        MigrateVolumeAnswer migrateVolumeAnswer;

        try {
            String value = _configDao.getValue(Config.PrimaryStorageDownloadWait.toString());
            int primaryStorageDownloadWait = NumbersUtil.parseInt(value, Integer.parseInt(Config.PrimaryStorageDownloadWait.getDefaultValue()));

            Map<String, String> details = getVolumeDetails(destVolumeInfo);

            MigrateVolumeCommand migrateVolumeCommand = new MigrateVolumeCommand(srcVolumeInfo.getTO(), destVolumeInfo.getTO(), details, primaryStorageDownloadWait);

            _volumeService.grantAccess(destVolumeInfo, hostVO, destVolumeInfo.getDataStore());

            migrateVolumeAnswer = (MigrateVolumeAnswer)_agentMgr.send(hostVO.getId(), migrateVolumeCommand);
        }
        catch (CloudRuntimeException | AgentUnavailableException | OperationTimedoutException ex) {
            String msg = "Failed to perform volume migration : ";

            LOGGER.warn(msg, ex);

            throw new CloudRuntimeException(msg + ex.getMessage());
        }
        finally {
            // If the source volume is attached to the volume, then keep the destination volume in the access control list.

            VirtualMachine virtualMachine = srcVolumeInfo.getAttachedVM();

            if (virtualMachine == null) {
                _volumeService.revokeAccess(destVolumeInfo, hostVO, destVolumeInfo.getDataStore());
            }
        }

        return migrateVolumeAnswer;
    }

    private boolean usingBackendSnapshotFor(SnapshotInfo snapshotInfo) {
        String property = getProperty(snapshotInfo.getId(), "takeSnapshot");

        return Boolean.parseBoolean(property);
    }

    private void handleCreateVolumeFromSnapshotBothOnStorageSystem(SnapshotInfo snapshotInfo, VolumeInfo volumeInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;
        CopyCmdAnswer copyCmdAnswer = null;

        try {
            boolean useCloning = usingBackendSnapshotFor(snapshotInfo);

            VolumeDetailVO volumeDetail = null;

            if (useCloning) {
                volumeDetail = new VolumeDetailVO(volumeInfo.getId(),
                        "cloneOfSnapshot",
                        String.valueOf(snapshotInfo.getId()),
                        false);

                volumeDetail = _volumeDetailsDao.persist(volumeDetail);
            }

            AsyncCallFuture<VolumeApiResult> future = _volumeService.createVolumeAsync(volumeInfo, volumeInfo.getDataStore());

            VolumeApiResult result = future.get();

            if (volumeDetail != null) {
                _volumeDetailsDao.remove(volumeDetail.getId());
            }

            if (result.isFailed()) {
                LOGGER.warn("Failed to create a volume: " + result.getResult());

                throw new CloudRuntimeException(result.getResult());
            }

            volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

            volumeInfo.processEvent(Event.MigrationRequested);

            volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

            VolumeObjectTO newVolume = new VolumeObjectTO();

            newVolume.setSize(volumeInfo.getSize());
            newVolume.setPath(volumeInfo.get_iScsiName());
            newVolume.setFormat(volumeInfo.getFormat());

            copyCmdAnswer = new CopyCmdAnswer(newVolume);
        }
        catch (Exception ex) {
            errMsg = "Copy operation failed in 'StorageSystemDataMotionStrategy.handleCreateVolumeFromSnapshotBothOnStorageSystem': " +
                    ex.getMessage();

            throw new CloudRuntimeException(errMsg);
        }
        finally {
            if (copyCmdAnswer == null) {
                copyCmdAnswer = new CopyCmdAnswer(errMsg);
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
    }

    private Map<String, String> getSnapshotDetails(SnapshotInfo snapshotInfo) {
        Map<String, String> snapshotDetails = new HashMap<>();

        long storagePoolId = snapshotInfo.getDataStore().getId();
        StoragePoolVO storagePoolVO = _storagePoolDao.findById(storagePoolId);

        snapshotDetails.put(DiskTO.STORAGE_HOST, storagePoolVO.getHostAddress());
        snapshotDetails.put(DiskTO.STORAGE_PORT, String.valueOf(storagePoolVO.getPort()));

        long snapshotId = snapshotInfo.getId();

        snapshotDetails.put(DiskTO.IQN, getProperty(snapshotId, DiskTO.IQN));

        snapshotDetails.put(DiskTO.CHAP_INITIATOR_USERNAME, getProperty(snapshotId, DiskTO.CHAP_INITIATOR_USERNAME));
        snapshotDetails.put(DiskTO.CHAP_INITIATOR_SECRET, getProperty(snapshotId, DiskTO.CHAP_INITIATOR_SECRET));
        snapshotDetails.put(DiskTO.CHAP_TARGET_USERNAME, getProperty(snapshotId, DiskTO.CHAP_TARGET_USERNAME));
        snapshotDetails.put(DiskTO.CHAP_TARGET_SECRET, getProperty(snapshotId, DiskTO.CHAP_TARGET_SECRET));

        return snapshotDetails;
    }

    private HostVO getHost(Long zoneId, HypervisorType hypervisorType) {
        Preconditions.checkArgument(zoneId != null, "Zone ID cannot be null.");
        Preconditions.checkArgument(hypervisorType != null, "Hypervisor type cannot be null.");

        List<HostVO> hosts = _hostDao.listByDataCenterId(zoneId);

        if (hosts == null) {
            return null;
        }

        Collections.shuffle(hosts, RANDOM);

        for (HostVO host : hosts) {
            if (hypervisorType.equals(host.getHypervisorType())) {
                return host;
            }
        }

        return null;
    }

    private HostVO getHostInCluster(long clusterId) {
        List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

        if (hosts != null && hosts.size() > 0) {
            Collections.shuffle(hosts, RANDOM);

            return hosts.get(0);
        }

        throw new CloudRuntimeException("Unable to locate a host");
    }

    private HostVO getHost(Long zoneId) {
        Preconditions.checkArgument(zoneId != null, "Zone ID cannot be null.");

        List<Long> hostIds = _hostDao.listAllHosts(zoneId);

        if (hostIds == null) {
            return null;
        }

        Collections.shuffle(hostIds, RANDOM);

        for (Long hostId : hostIds) {
            HostVO hostVO = _hostDao.findById(hostId);

            if (hostVO.getHypervisorType() == HypervisorType.KVM) {
                return hostVO;
            }
        }

        return null;
    }

    /**
     * If the underlying storage system is making use of read-only snapshots, this gives the storage system the opportunity to
     * create a volume from the snapshot so that we can copy the VHD file that should be inside of the snapshot to secondary storage.
     *
     * The resultant volume must be writable because we need to resign the SR and the VDI that should be inside of it before we copy
     * the VHD file to secondary storage.
     *
     * If the storage system is using writable snapshots, then nothing need be done by that storage system here because we can just
     * resign the SR and the VDI that should be inside of the snapshot before copying the VHD file to secondary storage.
     */
    private void createVolumeFromSnapshot(SnapshotInfo snapshotInfo) {
        SnapshotDetailsVO snapshotDetails = handleSnapshotDetails(snapshotInfo.getId(), "tempVolume", "create");

        try {
            snapshotInfo.getDataStore().getDriver().createAsync(snapshotInfo.getDataStore(), snapshotInfo, null);
        }
        finally {
            _snapshotDetailsDao.remove(snapshotDetails.getId());
        }
    }

    /**
     * If the underlying storage system needed to create a volume from a snapshot for createVolumeFromSnapshot(HostVO, SnapshotInfo), then
     * this is its opportunity to delete that temporary volume and restore properties in snapshot_details to the way they were before the
     * invocation of createVolumeFromSnapshot(HostVO, SnapshotInfo).
     */
    private void deleteVolumeFromSnapshot(SnapshotInfo snapshotInfo) {
        SnapshotDetailsVO snapshotDetails = handleSnapshotDetails(snapshotInfo.getId(), "tempVolume", "delete");

        try {
            snapshotInfo.getDataStore().getDriver().createAsync(snapshotInfo.getDataStore(), snapshotInfo, null);
        }
        finally {
            _snapshotDetailsDao.remove(snapshotDetails.getId());
        }
    }

    private SnapshotDetailsVO handleSnapshotDetails(long csSnapshotId, String name, String value) {
        _snapshotDetailsDao.removeDetail(csSnapshotId, name);

        SnapshotDetailsVO snapshotDetails = new SnapshotDetailsVO(csSnapshotId, name, value, false);

        return _snapshotDetailsDao.persist(snapshotDetails);
    }

    private boolean canStorageSystemCreateVolumeFromVolume(SnapshotInfo snapshotInfo) {
        boolean supportsCloningVolumeFromVolume = false;

        DataStore dataStore = dataStoreMgr.getDataStore(snapshotInfo.getDataStore().getId(), DataStoreRole.Primary);

        Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

        if (mapCapabilities != null) {
            String value = mapCapabilities.get(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_VOLUME.toString());

            supportsCloningVolumeFromVolume = Boolean.valueOf(value);
        }

        return supportsCloningVolumeFromVolume;
    }

    private String getProperty(long snapshotId, String property) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotId, property);

        if (snapshotDetails != null) {
            return snapshotDetails.getValue();
        }

        return null;
    }

    @Override
    public Void copyAsync(Map<VolumeInfo, DataStore> volumeMap, VirtualMachineTO vmTo, Host srcHost, Host destHost, AsyncCompletionCallback<CopyCommandResult> callback) {
        CopyCommandResult result = new CopyCommandResult(null, null);

        result.setResult("Unsupported operation requested for copying data.");

        callback.complete(result);

        return null;
    }

    private void handleCreateVolumeFromSnapshotOnSecondaryStorage(SnapshotInfo snapshotInfo, VolumeInfo volumeInfo,
                                                                  AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;
        CopyCmdAnswer copyCmdAnswer = null;

        try {
            // create a volume on the storage
            AsyncCallFuture<VolumeApiResult> future = _volumeService.createVolumeAsync(volumeInfo, volumeInfo.getDataStore());
            VolumeApiResult result = future.get();

            if (result.isFailed()) {
                LOGGER.error("Failed to create a volume: " + result.getResult());

                throw new CloudRuntimeException(result.getResult());
            }

            volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());
            volumeInfo.processEvent(Event.MigrationRequested);
            volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

            HostVO hostVO = getHost(snapshotInfo.getDataCenterId(), snapshotInfo.getHypervisorType());

            // copy the snapshot from secondary via the hypervisor
            copyCmdAnswer = copyImageToVolume(snapshotInfo, volumeInfo, hostVO);

            if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
                if (copyCmdAnswer != null && !StringUtils.isEmpty(copyCmdAnswer.getDetails())) {
                    throw new CloudRuntimeException(copyCmdAnswer.getDetails());
                }
                else {
                    throw new CloudRuntimeException("Unable to create volume from snapshot");
                }
            }
        }
        catch (Exception ex) {
            errMsg = "Copy operation failed in 'StorageSystemDataMotionStrategy.handleCreateVolumeFromSnapshotOnSecondaryStorage': " +
                    ex.getMessage();

            throw new CloudRuntimeException(errMsg);
        }
        finally {
            if (copyCmdAnswer == null) {
                copyCmdAnswer = new CopyCmdAnswer(errMsg);
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
    }

    private void handleCreateVolumeFromVolumeOnSecondaryStorage(VolumeInfo srcVolumeInfo, VolumeInfo destVolumeInfo,
                                                                long dataCenterId, HypervisorType hypervisorType,
                                                                AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;
        CopyCmdAnswer copyCmdAnswer = null;

        try {
            // create a volume on the storage
            destVolumeInfo.getDataStore().getDriver().createAsync(destVolumeInfo.getDataStore(), destVolumeInfo, null);

            destVolumeInfo = _volumeDataFactory.getVolume(destVolumeInfo.getId(), destVolumeInfo.getDataStore());

            HostVO hostVO = getHost(dataCenterId, hypervisorType);

            // copy the volume from secondary via the hypervisor
            copyCmdAnswer = copyImageToVolume(srcVolumeInfo, destVolumeInfo, hostVO);

            if (copyCmdAnswer == null || !copyCmdAnswer.getResult()) {
                if (copyCmdAnswer != null && !StringUtils.isEmpty(copyCmdAnswer.getDetails())) {
                    throw new CloudRuntimeException(copyCmdAnswer.getDetails());
                }
                else {
                    throw new CloudRuntimeException("Unable to create volume from volume");
                }
            }
        }
        catch (Exception ex) {
            errMsg = "Copy operation failed in 'StorageSystemDataMotionStrategy.handleCreateVolumeFromVolumeOnSecondaryStorage': " +
                    ex.getMessage();

            throw new CloudRuntimeException(errMsg);
        }
        finally {
            if (copyCmdAnswer == null) {
                copyCmdAnswer = new CopyCmdAnswer(errMsg);
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
    }

    private CopyCmdAnswer copyImageToVolume(DataObject srcDataObject, VolumeInfo destVolumeInfo, HostVO hostVO) {
        String value = _configDao.getValue(Config.PrimaryStorageDownloadWait.toString());
        int primaryStorageDownloadWait = NumbersUtil.parseInt(value, Integer.parseInt(Config.PrimaryStorageDownloadWait.getDefaultValue()));

        CopyCommand copyCommand = new CopyCommand(srcDataObject.getTO(), destVolumeInfo.getTO(), primaryStorageDownloadWait,
                VirtualMachineManager.ExecuteInSequence.value());

        CopyCmdAnswer copyCmdAnswer;

        try {
            _volumeService.grantAccess(destVolumeInfo, hostVO, destVolumeInfo.getDataStore());

            Map<String, String> destDetails = getVolumeDetails(destVolumeInfo);

            copyCommand.setOptions2(destDetails);

            copyCmdAnswer = (CopyCmdAnswer)_agentMgr.send(hostVO.getId(), copyCommand);
        }
        catch (CloudRuntimeException | AgentUnavailableException | OperationTimedoutException ex) {
            String msg = "Failed to copy image : ";

            LOGGER.warn(msg, ex);

            throw new CloudRuntimeException(msg + ex.getMessage());
        }
        finally {
            _volumeService.revokeAccess(destVolumeInfo, hostVO, destVolumeInfo.getDataStore());
        }

        VolumeObjectTO volumeObjectTO = (VolumeObjectTO)copyCmdAnswer.getNewData();

        volumeObjectTO.setFormat(ImageFormat.QCOW2);

        return copyCmdAnswer;
    }

    /**
     * Clones a template present on the storage to a new volume and resignatures it.
     *
     * @param templateInfo   source template
     * @param volumeInfo  destination ROOT volume
     * @param callback  for async
     */
    private void handleCreateVolumeFromTemplateBothOnStorageSystem(TemplateInfo templateInfo, VolumeInfo volumeInfo, AsyncCompletionCallback<CopyCommandResult> callback) {
        String errMsg = null;
        CopyCmdAnswer copyCmdAnswer = null;

        try {
            Preconditions.checkArgument(templateInfo != null, "Passing 'null' to templateInfo of " +
                    "handleCreateVolumeFromTemplateBothOnStorageSystem is not supported.");
            Preconditions.checkArgument(volumeInfo != null, "Passing 'null' to volumeInfo of " +
                    "handleCreateVolumeFromTemplateBothOnStorageSystem is not supported.");

            VolumeDetailVO volumeDetail = new VolumeDetailVO(volumeInfo.getId(),
                    "cloneOfTemplate",
                    String.valueOf(templateInfo.getId()),
                    false);

            volumeDetail = _volumeDetailsDao.persist(volumeDetail);

            AsyncCallFuture<VolumeApiResult> future = _volumeService.createVolumeAsync(volumeInfo, volumeInfo.getDataStore());
            VolumeApiResult result = future.get();

            if (volumeDetail != null) {
                _volumeDetailsDao.remove(volumeDetail.getId());
            }

            if (result.isFailed()) {
                LOGGER.warn("Failed to create a volume: " + result.getResult());

                throw new CloudRuntimeException(result.getResult());
            }

            volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

            volumeInfo.processEvent(Event.MigrationRequested);

            volumeInfo = _volumeDataFactory.getVolume(volumeInfo.getId(), volumeInfo.getDataStore());

            VolumeObjectTO newVolume = new VolumeObjectTO();

            newVolume.setSize(volumeInfo.getSize());
            newVolume.setPath(volumeInfo.getPath());
            newVolume.setFormat(volumeInfo.getFormat());

            copyCmdAnswer = new CopyCmdAnswer(newVolume);
        } catch (Exception ex) {
            try {
                volumeInfo.getDataStore().getDriver().deleteAsync(volumeInfo.getDataStore(), volumeInfo, null);
            }
            catch (Exception exc) {
                LOGGER.warn("Failed to delete volume", exc);
            }

            errMsg = "Create volume from template (ID = " + templateInfo.getId() + ") failed: " + ex.getMessage();

            throw new CloudRuntimeException(errMsg);
        }
        finally {
            if (copyCmdAnswer == null) {
                copyCmdAnswer = new CopyCmdAnswer(errMsg);
            }

            CopyCommandResult result = new CopyCommandResult(null, copyCmdAnswer);

            result.setResult(errMsg);

            callback.complete(result);
        }
    }
}
