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
package org.apache.cloudstack.storage.snapshot;

import java.util.Map;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreCapabilities;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.ObjectInDataStoreStateMachine;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotResult;
import org.apache.cloudstack.engine.subsystem.api.storage.StrategyPriority;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;

import org.springframework.stereotype.Component;

import com.cloud.exception.InvalidParameterValueException;
import com.cloud.hypervisor.Hypervisor.HypervisorType;
import com.cloud.storage.DataStoreRole;
import com.cloud.storage.Snapshot;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.Storage.ImageFormat;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.utils.db.DB;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.fsm.NoTransitionException;

@Component
public class StorageSystemSnapshotStrategy extends SnapshotStrategyBase {
    private static final Logger s_logger = Logger.getLogger(StorageSystemSnapshotStrategy.class);

    @Inject private DataStoreManager dataStoreMgr;
    @Inject private PrimaryDataStoreDao storagePoolDao;
    @Inject private SnapshotDao snapshotDao;
    @Inject private SnapshotDataFactory snapshotDataFactory;
    @Inject private SnapshotDetailsDao snapshotDetailsDao;
    @Inject SnapshotDataStoreDao snapshotStoreDao;
    @Inject private VolumeDao volumeDao;

    @Override
    public SnapshotInfo backupSnapshot(SnapshotInfo snapshotInfo) {
        return snapshotInfo;
    }

    @Override
    public boolean deleteSnapshot(Long snapshotId) {
        SnapshotVO snapshotVO = snapshotDao.findById(snapshotId);

        if (Snapshot.State.Destroyed.equals(snapshotVO.getState())) {
            return true;
        }

        if (Snapshot.State.Error.equals(snapshotVO.getState())) {
            snapshotDao.remove(snapshotId);

            return true;
        }

        if (!Snapshot.State.BackedUp.equals(snapshotVO.getState())) {
            throw new InvalidParameterValueException("Unable to delete snapshotshot " + snapshotId + " because it is in the following state: " + snapshotVO.getState());
        }

        SnapshotObject snapshotObj = (SnapshotObject)snapshotDataFactory.getSnapshot(snapshotId, DataStoreRole.Primary);

        if (snapshotObj == null) {
            s_logger.debug("Can't find snapshot; deleting it in DB");

            snapshotDao.remove(snapshotId);

            return true;
        }

        if (ObjectInDataStoreStateMachine.State.Copying.equals(snapshotObj.getStatus())) {
            throw new InvalidParameterValueException("Unable to delete snapshotshot " + snapshotId + " because it is in the copying state.");
        }

        try {
            snapshotObj.processEvent(Snapshot.Event.DestroyRequested);
        }
        catch (NoTransitionException e) {
            s_logger.debug("Failed to set the state to destroying: ", e);

            return false;
        }

        try {
            snapshotSvr.deleteSnapshot(snapshotObj);

            snapshotObj.processEvent(Snapshot.Event.OperationSucceeded);
        }
        catch (Exception e) {
            s_logger.debug("Failed to delete snapshot: ", e);

            try {
                snapshotObj.processEvent(Snapshot.Event.OperationFailed);
            }
            catch (NoTransitionException e1) {
                s_logger.debug("Failed to change snapshot state: " + e.toString());
            }

            return false;
        }

        return true;
    }

    private void verifySnapshotType(SnapshotInfo snapshotInfo) {
        if (snapshotInfo.getHypervisorType() == HypervisorType.KVM && snapshotInfo.getDataStore().getRole() != DataStoreRole.Primary) {
            throw new CloudRuntimeException("For the KVM hypervisor type, you can only revert a volume to a snapshot state if the snapshot " +
                    "resides on primary storage. For other snapshot types, create a volume from the snapshot to recover its data.");
        }
    }

    @Override
    public boolean revertSnapshot(SnapshotInfo snapshotInfo) {
        verifySnapshotType(snapshotInfo);

        VolumeInfo volumeInfo = snapshotInfo.getBaseVolume();

        if (volumeInfo.getFormat() != ImageFormat.QCOW2) {
            throw new CloudRuntimeException("Only the " + ImageFormat.QCOW2.toString() + " image type is currently supported.");
        }

        SnapshotVO snapshotVO = snapshotDao.acquireInLockTable(snapshotInfo.getId());

        if (snapshotVO == null) {
            throw new CloudRuntimeException("Failed to acquire lock on the following snapshot: " + snapshotInfo.getId());
        }

        boolean success = false;

        try {
            volumeInfo.stateTransit(Volume.Event.RevertSnapshotRequested);

            success = snapshotSvr.revertSnapshot(snapshotInfo);

            if (!success) {
                String errMsg = "Failed to revert a volume to a snapshot state";

                s_logger.debug(errMsg);

                throw new CloudRuntimeException(errMsg);
            }
        }
        finally {
            if (success) {
                volumeInfo.stateTransit(Volume.Event.OperationSucceeded);
            }
            else {
                volumeInfo.stateTransit(Volume.Event.OperationFailed);
            }

            snapshotDao.releaseFromLockTable(snapshotInfo.getId());
        }

        return true;
    }

    @Override
    @DB
    public SnapshotInfo takeSnapshot(SnapshotInfo snapshotInfo) {
        VolumeInfo volumeInfo = snapshotInfo.getBaseVolume();

        if (volumeInfo.getFormat() != ImageFormat.QCOW2) {
            throw new CloudRuntimeException("Only the " + ImageFormat.QCOW2.toString() + " image type is currently supported.");
        }

        SnapshotVO snapshotVO = snapshotDao.acquireInLockTable(snapshotInfo.getId());

        if (snapshotVO == null) {
            throw new CloudRuntimeException("Failed to acquire lock on the following snapshot: " + snapshotInfo.getId());
        }

        SnapshotResult result = null;

        try {
            volumeInfo.stateTransit(Volume.Event.SnapshotRequested);

            boolean canStorageSystemCreateVolumeFromSnapshot = canStorageSystemCreateVolumeFromSnapshot(volumeInfo.getPoolId());

            if (canStorageSystemCreateVolumeFromSnapshot) {
                SnapshotDetailsVO snapshotDetail = new SnapshotDetailsVO(snapshotInfo.getId(),
                    "takeSnapshot",
                    Boolean.TRUE.toString(),
                    false);

                snapshotDetailsDao.persist(snapshotDetail);
            }

            result = snapshotSvr.takeSnapshot(snapshotInfo);

            if (result.isFailed()) {
                s_logger.debug("Failed to take a snapshot: " + result.getResult());

                throw new CloudRuntimeException(result.getResult());
            }

            markAsBackedUp((SnapshotObject)result.getSnashot());
        }
        finally {
            if (result != null && result.isSuccess()) {
                volumeInfo.stateTransit(Volume.Event.OperationSucceeded);
            }
            else {
                volumeInfo.stateTransit(Volume.Event.OperationFailed);
            }

            snapshotDao.releaseFromLockTable(snapshotInfo.getId());
        }

        return snapshotInfo;
    }

    private void markAsBackedUp(SnapshotObject snapshotObj) {
        try {
            snapshotObj.processEvent(Snapshot.Event.BackupToSecondary);
            snapshotObj.processEvent(Snapshot.Event.OperationSucceeded);
        }
        catch (NoTransitionException ex) {
            s_logger.debug("Failed to change state: " + ex.toString());

            try {
                snapshotObj.processEvent(Snapshot.Event.OperationFailed);
            }
            catch (NoTransitionException ex2) {
                s_logger.debug("Failed to change state: " + ex2.toString());
            }
        }
    }

    private String getProperty(long snapshotId, String property) {
        SnapshotDetailsVO snapshotDetails = snapshotDetailsDao.findDetail(snapshotId, property);

        if (snapshotDetails != null) {
            return snapshotDetails.getValue();
        }

        return null;
    }

    private boolean canStorageSystemCreateVolumeFromSnapshot(long storagePoolId) {
        return storageSystemSupportsCapability(storagePoolId, DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_SNAPSHOT.toString());
    }

    private boolean canStorageSystemRevertVolumeToSnapshot(long storagePoolId) {
        return storageSystemSupportsCapability(storagePoolId, DataStoreCapabilities.CAN_REVERT_VOLUME_TO_SNAPSHOT.toString());
    }

    private boolean storageSystemSupportsCapability(long storagePoolId, String capability) {
        boolean supportsCapability = false;

        DataStore dataStore = dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);

        Map<String, String> mapCapabilities = dataStore.getDriver().getCapabilities();

        if (mapCapabilities != null) {
            String value = mapCapabilities.get(capability);

            supportsCapability = Boolean.valueOf(value);
        }

        return supportsCapability;
    }

    private boolean usingBackendSnapshotFor(long snapshotId) {
        String property = getProperty(snapshotId, "takeSnapshot");

        return Boolean.parseBoolean(property);
    }

    @Override
    public StrategyPriority canHandle(Snapshot snapshot, SnapshotOperation op) {
        // If the snapshot exists on Secondary Storage, we can't delete it.
        if (SnapshotOperation.DELETE.equals(op)) {
            SnapshotDataStoreVO snapshotStore = snapshotStoreDao.findBySnapshot(snapshot.getId(), DataStoreRole.Image);

            if (snapshotStore != null) {
                return StrategyPriority.CANT_HANDLE;
            }
        }

        long volumeId = snapshot.getVolumeId();

        VolumeVO volumeVO = volumeDao.findByIdIncludingRemoved(volumeId);

        long storagePoolId = volumeVO.getPoolId();

        if (SnapshotOperation.REVERT.equals(op)) {
            boolean baseVolumeExists = volumeVO.getRemoved() == null;

            if (baseVolumeExists) {
                boolean acceptableFormat = ImageFormat.QCOW2.equals(volumeVO.getFormat());

                if (acceptableFormat) {
                    boolean usingBackendSnapshot = usingBackendSnapshotFor(snapshot.getId());

                    if (usingBackendSnapshot) {
                        boolean storageSystemSupportsCapability = storageSystemSupportsCapability(storagePoolId, DataStoreCapabilities.CAN_REVERT_VOLUME_TO_SNAPSHOT.toString());

                        if (storageSystemSupportsCapability) {
                            return StrategyPriority.HIGHEST;
                        }
                    }
                    else {
                        StoragePoolVO storagePoolVO = storagePoolDao.findById(storagePoolId);

                        if (storagePoolVO.isManaged()) {
                            return StrategyPriority.HIGHEST;
                        }
                    }
                }
            }

            return StrategyPriority.CANT_HANDLE;
        }

        boolean storageSystemSupportsCapability = storageSystemSupportsCapability(storagePoolId, DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString());

        return storageSystemSupportsCapability ? StrategyPriority.HIGHEST : StrategyPriority.CANT_HANDLE;
    }
}
