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
package org.apache.cloudstack.storage.datastore.driver;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.cloudstack.engine.subsystem.api.storage.ChapInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.CreateCmdResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreCapabilities;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreDriver;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.TemplateInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.storage.command.CreateObjectAnswer;
import org.apache.cloudstack.storage.command.CommandResult;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.datastore.util.SolidFireUtil;
import org.apache.cloudstack.storage.to.SnapshotObjectTO;
import org.apache.log4j.Logger;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.to.DataObjectType;
import com.cloud.agent.api.to.DataStoreTO;
import com.cloud.agent.api.to.DataTO;
import com.cloud.agent.api.to.DiskTO;
import com.cloud.dc.ClusterVO;
import com.cloud.dc.ClusterDetailsVO;
import com.cloud.dc.ClusterDetailsDao;
import com.cloud.dc.dao.ClusterDao;
import com.cloud.host.Host;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.hypervisor.Hypervisor;
import com.cloud.storage.DataStoreRole;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.ResizeVolumePayload;
import com.cloud.storage.Snapshot.State;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.StoragePool;
import com.cloud.storage.VMTemplateStoragePoolVO;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
import com.cloud.storage.dao.VMTemplatePoolDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.dao.VolumeDetailsDao;
import com.cloud.user.AccountDetailVO;
import com.cloud.user.AccountDetailsDao;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.utils.db.GlobalLock;
import com.cloud.utils.exception.CloudRuntimeException;

public class SolidFirePrimaryDataStoreDriver implements PrimaryDataStoreDriver {
    private static final Logger s_logger = Logger.getLogger(SolidFirePrimaryDataStoreDriver.class);

    private static final int LOWEST_HYPERVISOR_SNAPSHOT_RESERVE = 10;
    private static final long MIN_IOPS_FOR_TEMPLATE_VOLUME = 100L;
    private static final long MAX_IOPS_FOR_TEMPLATE_VOLUME = 20000L;
    private static final long MIN_IOPS_FOR_TEMP_VOLUME = 100L;
    private static final long MAX_IOPS_FOR_TEMP_VOLUME = 20000L;
    private static final long MIN_IOPS_FOR_SNAPSHOT_VOLUME = 100L;
    private static final long MAX_IOPS_FOR_SNAPSHOT_VOLUME = 20000L;

    @Inject private AccountDao _accountDao;
    @Inject private AccountDetailsDao _accountDetailsDao;
    @Inject private ClusterDao _clusterDao;
    @Inject private ClusterDetailsDao _clusterDetailsDao;
    @Inject private DataStoreManager _dataStoreMgr;
    @Inject private HostDao _hostDao;
    @Inject private SnapshotDao _snapshotDao;
    @Inject private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private StoragePoolDetailsDao _storagePoolDetailsDao;
    @Inject private VMTemplatePoolDao _tmpltPoolDao;
    @Inject private VolumeDao _volumeDao;
    @Inject private VolumeDetailsDao _volumeDetailsDao;
    @Inject private VolumeDataFactory _volumeFactory;

    @Override
    public Map<String, String> getCapabilities() {
        Map<String, String> mapCapabilities = new HashMap<>();

        mapCapabilities.put(DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString(), Boolean.TRUE.toString());
        mapCapabilities.put(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_SNAPSHOT.toString(), Boolean.TRUE.toString());
        mapCapabilities.put(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_VOLUME.toString(), Boolean.TRUE.toString());
        mapCapabilities.put(DataStoreCapabilities.CAN_REVERT_VOLUME_TO_SNAPSHOT.toString(), Boolean.TRUE.toString());

        return mapCapabilities;
    }

    @Override
    public DataTO getTO(DataObject data) {
        return null;
    }

    @Override
    public DataStoreTO getStoreTO(DataStore store) {
        return null;
    }

    private SolidFireUtil.SolidFireAccount createSolidFireAccount(SolidFireUtil.SolidFireConnection sfConnection, String sfAccountName) {
        long accountNumber = SolidFireUtil.createSolidFireAccount(sfConnection, sfAccountName);

        return SolidFireUtil.getSolidFireAccountById(sfConnection, accountNumber);
    }

    @Override
    public ChapInfo getChapInfo(DataObject dataObject) {
        return null;
    }

    // get the VAG associated with volumeInfo's cluster, if any (ListVolumeAccessGroups)
    // if the VAG exists
    //     update the VAG to contain all IQNs of the hosts (ModifyVolumeAccessGroup)
    //     if the ID of volumeInfo in not in the VAG, add it (ModifyVolumeAccessGroup)
    // if the VAG doesn't exist, create it with the IQNs of the hosts and the ID of volumeInfo (CreateVolumeAccessGroup)
    @Override
    public boolean grantAccess(DataObject dataObject, Host host, DataStore dataStore)
    {
        if (dataObject == null || host == null || dataStore == null) {
            return false;
        }

        long sfVolumeId = getSolidFireVolumeId(dataObject);
        long clusterId = host.getClusterId();
        long storagePoolId = dataStore.getId();

        ClusterVO cluster = _clusterDao.findById(clusterId);

        GlobalLock lock = GlobalLock.getInternLock(cluster.getUuid());

        if (!lock.lock(SolidFireUtil.LOCK_TIME_IN_SECONDS)) {
            String errMsg = "Couldn't lock the DB (in connectVolumeToHost) on the following string: " + cluster.getUuid();

            s_logger.warn(errMsg);

            throw new CloudRuntimeException(errMsg);
        }

        try {
            ClusterDetailsVO clusterDetail = _clusterDetailsDao.findDetail(clusterId, SolidFireUtil.getVagKey(storagePoolId));

            String vagId = clusterDetail != null ? clusterDetail.getValue() : null;

            List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

            if (!SolidFireUtil.hostsSupport_iScsi(hosts)) {
                String errMsg = "Not all hosts in the compute cluster support iSCSI.";

                s_logger.warn(errMsg);

                throw new CloudRuntimeException(errMsg);
            }

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            if (vagId != null) {
                SolidFireUtil.SolidFireVag sfVag = SolidFireUtil.getSolidFireVag(sfConnection, Long.parseLong(vagId));

                long[] volumeIds = SolidFireUtil.getNewVolumeIds(sfVag.getVolumeIds(), sfVolumeId, true);

                SolidFireUtil.modifySolidFireVag(sfConnection, sfVag.getId(), sfVag.getInitiators(), volumeIds);
            }
            else {
                SolidFireUtil.placeVolumeInVolumeAccessGroup(sfConnection, sfVolumeId, storagePoolId, cluster.getUuid(), hosts, _clusterDetailsDao);
            }

            return true;
        }
        finally {
            lock.unlock();
            lock.releaseRef();
        }
    }

    // get the VAG associated with volumeInfo's cluster, if any (ListVolumeAccessGroups) // might not exist if using CHAP
    // if the VAG exists
    //     remove the ID of volumeInfo from the VAG (ModifyVolumeAccessGroup)
    @Override
    public void revokeAccess(DataObject dataObject, Host host, DataStore dataStore)
    {
        if (dataObject == null || host == null || dataStore == null) {
            return;
        }

        long sfVolumeId = getSolidFireVolumeId(dataObject);
        long clusterId = host.getClusterId();
        long storagePoolId = dataStore.getId();

        ClusterVO cluster = _clusterDao.findById(clusterId);

        GlobalLock lock = GlobalLock.getInternLock(cluster.getUuid());

        if (!lock.lock(SolidFireUtil.LOCK_TIME_IN_SECONDS)) {
            String errMsg = "Couldn't lock the DB (in disconnectVolumeFromHost) on the following string: " + cluster.getUuid();

            s_logger.debug(errMsg);

            throw new CloudRuntimeException(errMsg);
        }

        try {
            ClusterDetailsVO clusterDetail = _clusterDetailsDao.findDetail(clusterId, SolidFireUtil.getVagKey(storagePoolId));

            String vagId = clusterDetail != null ? clusterDetail.getValue() : null;

            if (vagId != null) {
                SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

                SolidFireUtil.SolidFireVag sfVag = SolidFireUtil.getSolidFireVag(sfConnection, Long.parseLong(vagId));

                long[] volumeIds = SolidFireUtil.getNewVolumeIds(sfVag.getVolumeIds(), sfVolumeId, false);

                SolidFireUtil.modifySolidFireVag(sfConnection, sfVag.getId(), sfVag.getInitiators(), volumeIds);
            }
        }
        finally {
            lock.unlock();
            lock.releaseRef();
        }
    }

    private long getSolidFireVolumeId(DataObject dataObject) {
        if (dataObject.getType() == DataObjectType.VOLUME) {
            final VolumeInfo volumeInfo = (VolumeInfo)dataObject;

            return Long.parseLong(volumeInfo.getFolder());
        }

        if (dataObject.getType() == DataObjectType.SNAPSHOT) {
            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(dataObject.getId(), SolidFireUtil.VOLUME_ID);

            if (snapshotDetails == null || snapshotDetails.getValue() == null) {
                throw new CloudRuntimeException("Unable to locate the volume ID associated with the following snapshot ID: " + dataObject.getId());
            }

            return Long.parseLong(snapshotDetails.getValue());
        }

        if (dataObject.getType() == DataObjectType.TEMPLATE) {
            return getVolumeIdFrom_iScsiPath(((TemplateInfo)dataObject).getInstallPath());
        }

        throw new CloudRuntimeException("Invalid DataObjectType (" + dataObject.getType() + ") passed to getSolidFireVolumeId(DataObject, boolean)");
    }

    private long getVolumeIdFrom_iScsiPath(String iScsiPath) {
        String[] splits = iScsiPath.split("/");
        String iqn = splits[1];

        String sequenceToSearchFor = ".";
        int lastIndexOf = iqn.lastIndexOf(sequenceToSearchFor);
        String volumeIdAsString = iqn.substring(lastIndexOf + sequenceToSearchFor.length());

        return Long.parseLong(volumeIdAsString);
    }

    private long getDefaultMinIops(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, SolidFireUtil.CLUSTER_DEFAULT_MIN_IOPS);

        String clusterDefaultMinIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMinIops);
    }

    private long getDefaultMaxIops(long storagePoolId) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, SolidFireUtil.CLUSTER_DEFAULT_MAX_IOPS);

        String clusterDefaultMaxIops = storagePoolDetail.getValue();

        return Long.parseLong(clusterDefaultMaxIops);
    }

    private long getDefaultBurstIops(long storagePoolId, long maxIops) {
        StoragePoolDetailVO storagePoolDetail = _storagePoolDetailsDao.findDetail(storagePoolId, SolidFireUtil.CLUSTER_DEFAULT_BURST_IOPS_PERCENT_OF_MAX_IOPS);

        String clusterDefaultBurstIopsPercentOfMaxIops = storagePoolDetail.getValue();

        float fClusterDefaultBurstIopsPercentOfMaxIops = Float.parseFloat(clusterDefaultBurstIopsPercentOfMaxIops);

        return Math.min((long)(maxIops * fClusterDefaultBurstIopsPercentOfMaxIops), SolidFireUtil.MAX_IOPS_PER_VOLUME);
    }

    private SolidFireUtil.SolidFireVolume createSolidFireVolume(SolidFireUtil.SolidFireConnection sfConnection, DataObject dataObject,
                                                                long storagePoolId, long sfAccountId) {
        final Long minIops;
        final Long maxIops;
        final Long volumeSize;
        final String volumeName;

        final Map<String, String> mapAttributes;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            VolumeInfo volumeInfo = (VolumeInfo)dataObject;

            minIops = volumeInfo.getMinIops();
            maxIops = volumeInfo.getMaxIops();
            volumeSize = getDataObjectSizeIncludingHypervisorSnapshotReserve(volumeInfo, _storagePoolDao.findById(storagePoolId));
            volumeName = volumeInfo.getName();

            mapAttributes = getVolumeAttributes(volumeInfo);
        } else if (dataObject.getType() == DataObjectType.TEMPLATE) {
            TemplateInfo templateInfo = (TemplateInfo)dataObject;

            minIops = MIN_IOPS_FOR_TEMPLATE_VOLUME;
            maxIops = MAX_IOPS_FOR_TEMPLATE_VOLUME;
            volumeSize = getDataObjectSizeIncludingHypervisorSnapshotReserve(templateInfo, _storagePoolDao.findById(storagePoolId));
            volumeName = templateInfo.getUniqueName();

            mapAttributes = getTemplateAttributes(templateInfo);
        }
        else {
            throw new CloudRuntimeException("Invalid type passed to createSolidFireVolume: " + dataObject.getType());
        }

        final Iops iops = getIops(minIops, maxIops, storagePoolId);

        long sfVolumeId = SolidFireUtil.createSolidFireVolume(sfConnection, SolidFireUtil.getSolidFireVolumeName(volumeName), sfAccountId,
                volumeSize, true, mapAttributes, iops.getMinIops(), iops.getMaxIops(), iops.getBurstIops());

        return SolidFireUtil.getSolidFireVolume(sfConnection, sfVolumeId);
    }

    @Override
    public long getUsedBytes(StoragePool storagePool) {
        return getUsedBytes(storagePool, Long.MIN_VALUE);
    }

    private long getUsedBytes(StoragePool storagePool, long volumeIdToIgnore) {
        long usedSpace = 0;

        List<VolumeVO> lstVolumes = _volumeDao.findByPoolId(storagePool.getId(), null);

        if (lstVolumes != null) {
            for (VolumeVO volume : lstVolumes) {
                if (volume.getId() == volumeIdToIgnore) {
                    continue;
                }

                VolumeDetailVO volumeDetail = _volumeDetailsDao.findDetail(volume.getId(), SolidFireUtil.VOLUME_SIZE);

                if (volumeDetail != null && volumeDetail.getValue() != null) {
                    long volumeSize = Long.parseLong(volumeDetail.getValue());

                    usedSpace += volumeSize;
                }
                else {
                    SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePool.getId(), _storagePoolDetailsDao);

                    try {
                        long lVolumeId = Long.parseLong(volume.getFolder());

                        SolidFireUtil.SolidFireVolume sfVolume = SolidFireUtil.getSolidFireVolume(sfConnection, lVolumeId);

                        long volumeSize = sfVolume.getTotalSize();

                        // SolidFireUtil.VOLUME_SIZE was introduced in 4.5.
                        // To be backward compatible with releases prior to 4.5, call updateVolumeDetails here.
                        // That way if SolidFireUtil.VOLUME_SIZE wasn't put in the volume_details table when the
                        // volume was initially created, it can be placed in volume_details here.
                        updateVolumeDetails(volume.getId(), volumeSize);

                        usedSpace += volumeSize;
                    }
                    catch (NumberFormatException ex) {
                        // can be ignored (the "folder" column didn't have a valid "long" in it (hasn't been placed there yet))
                    }
                }
            }
        }

        List<SnapshotVO> lstSnapshots = _snapshotDao.listAll();

        if (lstSnapshots != null) {
            for (SnapshotVO snapshot : lstSnapshots) {
                SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshot.getId(), SolidFireUtil.STORAGE_POOL_ID);

                // if this snapshot belongs to the storagePool that was passed in
                if (snapshotDetails != null && snapshotDetails.getValue() != null && Long.parseLong(snapshotDetails.getValue()) == storagePool.getId()) {
                    snapshotDetails = _snapshotDetailsDao.findDetail(snapshot.getId(), SolidFireUtil.VOLUME_SIZE);

                    if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                        long snapshotSize = Long.parseLong(snapshotDetails.getValue());

                        usedSpace += snapshotSize;
                    }
                }
            }
        }

        List<VMTemplateStoragePoolVO> lstTemplatePoolRefs = _tmpltPoolDao.listByPoolId(storagePool.getId());

        if (lstTemplatePoolRefs != null) {
            for (VMTemplateStoragePoolVO templatePoolRef : lstTemplatePoolRefs) {
                usedSpace += templatePoolRef.getTemplateSize();
            }
        }

        return usedSpace;
    }

    @Override
    public long getDataObjectSizeIncludingHypervisorSnapshotReserve(DataObject dataObject, StoragePool pool) {
        long volumeSize = 0;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            VolumeInfo volume = (VolumeInfo)dataObject;

            volumeSize = getVolumeSizeIncludingHypervisorSnapshotReserve(volume.getSize(), volume.getHypervisorSnapshotReserve());
        } else if (dataObject.getType() == DataObjectType.TEMPLATE) {
            TemplateInfo templateInfo = (TemplateInfo)dataObject;

            // TemplateInfo sometimes has a size equal to null.
            long templateSize = templateInfo.getSize() != null ? templateInfo.getSize() : 0;

            if (templateInfo.getHypervisorType() == Hypervisor.HypervisorType.KVM) {
                volumeSize = templateSize;
            }
            else {
                volumeSize = (long)(templateSize + templateSize * (LOWEST_HYPERVISOR_SNAPSHOT_RESERVE / 100f));
            }
        }

        return Math.max(volumeSize, SolidFireUtil.MIN_VOLUME_SIZE);
    }

    private long getVolumeSizeIncludingHypervisorSnapshotReserve(long volumeSize, Integer hypervisorSnapshotReserve) {
        if (hypervisorSnapshotReserve != null) {
            hypervisorSnapshotReserve = Math.max(hypervisorSnapshotReserve, LOWEST_HYPERVISOR_SNAPSHOT_RESERVE);

            volumeSize += volumeSize * (hypervisorSnapshotReserve / 100f);
        }

        return volumeSize;
    }

    private static class Iops {
        private final long _minIops;
        private final long _maxIops;
        private final long _burstIops;

        Iops(long minIops, long maxIops, long burstIops) throws IllegalArgumentException {
            if (minIops <= 0 || maxIops <= 0) {
                throw new IllegalArgumentException("The 'Min IOPS' and 'Max IOPS' values must be greater than 0.");
            }

            if (minIops > maxIops) {
                throw new IllegalArgumentException("The 'Min IOPS' value cannot exceed the 'Max IOPS' value.");
            }

            if (maxIops > burstIops) {
                throw new IllegalArgumentException("The 'Max IOPS' value cannot exceed the 'Burst IOPS' value.");
            }

            _minIops = minIops;
            _maxIops = maxIops;
            _burstIops = burstIops;
        }

        long getMinIops() {
            return _minIops;
        }

        long getMaxIops() {
            return _maxIops;
        }

        long getBurstIops() {
            return _burstIops;
        }
    }

    private void deleteSolidFireVolume(SolidFireUtil.SolidFireConnection sfConnection, VolumeInfo volumeInfo)
    {
        Long storagePoolId = volumeInfo.getPoolId();

        if (storagePoolId == null) {
            return; // this volume was never assigned to a storage pool, so no SAN volume should exist for it
        }

        long sfVolumeId = Long.parseLong(volumeInfo.getFolder());

        deleteSolidFireVolume(sfConnection, volumeInfo.getId(), sfVolumeId);
    }

    private void deleteSolidFireVolume(SolidFireUtil.SolidFireConnection sfConnection, long csVolumeId, long sfVolumeId) {
        List<SnapshotVO> lstSnapshots = getNonDestroyedSnapshots(csVolumeId);

        boolean deleteVolume = true;

        for (SnapshotVO snapshot : lstSnapshots) {
            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshot.getId(), SolidFireUtil.SNAPSHOT_ID);

            if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                deleteVolume = false;

                break;
            }
        }

        if (deleteVolume) {
            SolidFireUtil.deleteSolidFireVolume(sfConnection, sfVolumeId);
        }
    }

    @Override
    public void createAsync(DataStore dataStore, DataObject dataObject, AsyncCompletionCallback<CreateCmdResult> callback) {
        String iqn = null;
        String errMsg = null;

        try {
            if (dataObject.getType() == DataObjectType.VOLUME) {
                iqn = createVolume((VolumeInfo)dataObject, dataStore.getId());
            } else if (dataObject.getType() == DataObjectType.SNAPSHOT) {
                createTempVolume((SnapshotInfo)dataObject, dataStore.getId());
            } else if (dataObject.getType() == DataObjectType.TEMPLATE) {
                iqn = createTemplateVolume((TemplateInfo)dataObject, dataStore.getId());
            } else {
                errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to createAsync";

                s_logger.error(errMsg);
            }
        }
        catch (Exception ex) {
            errMsg = ex.getMessage();

            s_logger.error(errMsg);

            if (callback == null) {
                throw ex;
            }
        }

        if (callback != null) {
            // path = iqn
            // size is pulled from DataObject instance, if errMsg is null
            CreateCmdResult result = new CreateCmdResult(iqn, new Answer(null, errMsg == null, errMsg));

            result.setResult(errMsg);

            callback.complete(result);
        }
        else {
            if (errMsg != null) {
                throw new CloudRuntimeException(errMsg);
            }
        }
    }

    private String createVolume(VolumeInfo volumeInfo, long storagePoolId) {
        verifySufficientBytesForStoragePool(volumeInfo, storagePoolId);
        verifySufficientIopsForStoragePool(volumeInfo.getMinIops() != null ? volumeInfo.getMinIops() : getDefaultMinIops(storagePoolId), storagePoolId);

        SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

        long sfAccountId = getCreateSolidFireAccountId(sfConnection, volumeInfo.getAccountId(), storagePoolId);

        SolidFireUtil.SolidFireVolume sfVolume;

        long csSnapshotId = getCsIdForCloning(volumeInfo.getId(), "cloneOfSnapshot");
        long csTemplateId = getCsIdForCloning(volumeInfo.getId(), "cloneOfTemplate");

        if (csSnapshotId > 0) {
            // We are supposed to create a clone of the underlying volume or snapshot that supports the CloudStack snapshot.
            sfVolume = createClone(sfConnection, csSnapshotId, volumeInfo, sfAccountId, storagePoolId, DataObjectType.SNAPSHOT);
        } else if (csTemplateId > 0) {
            // Clone from template.
            sfVolume = createClone(sfConnection, csTemplateId, volumeInfo, sfAccountId, storagePoolId, DataObjectType.TEMPLATE);

            long volumeSize = getDataObjectSizeIncludingHypervisorSnapshotReserve(volumeInfo, _storagePoolDao.findById(storagePoolId));

            if (volumeSize > sfVolume.getTotalSize()) {
                // Expand the volume to include HSR.
                SolidFireUtil.modifySolidFireVolume(sfConnection, sfVolume.getId(), volumeSize, getVolumeAttributes(volumeInfo),
                        sfVolume.getMinIops(), sfVolume.getMaxIops(), sfVolume.getBurstIops());

                // Get the SolidFire volume from the SAN again because we just updated its size.
                sfVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfVolume.getId());
            }
        }
        else {
            sfVolume = createSolidFireVolume(sfConnection, volumeInfo, storagePoolId, sfAccountId);
        }

        String iqn = sfVolume.getIqn();

        VolumeVO volume = _volumeDao.findById(volumeInfo.getId());

        volume.set_iScsiName(iqn);
        volume.setFolder(String.valueOf(sfVolume.getId()));
        volume.setPoolType(StoragePoolType.IscsiLUN);
        volume.setPoolId(storagePoolId);

        _volumeDao.update(volume.getId(), volume);

        updateVolumeDetails(volume.getId(), sfVolume.getTotalSize());

        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

        long capacityBytes = storagePool.getCapacityBytes();
        // getUsedBytes(StoragePool) will include the bytes of the newly created volume because
        // updateVolumeDetails(long, long) has already been called for this volume
        long usedBytes = getUsedBytes(storagePool);

        storagePool.setUsedBytes(usedBytes > capacityBytes ? capacityBytes : usedBytes);

        _storagePoolDao.update(storagePoolId, storagePool);

        return iqn;
    }

    private void createTempVolume(SnapshotInfo snapshotInfo, long storagePoolId) {
        long csSnapshotId = snapshotInfo.getId();

        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

        if (snapshotDetails == null || snapshotDetails.getValue() == null) {
            throw new CloudRuntimeException("'createTempVolume(SnapshotInfo, long)' should not be invoked unless " + SolidFireUtil.SNAPSHOT_ID + " exists.");
        }

        SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

        snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, "tempVolume");

        if (snapshotDetails != null && snapshotDetails.getValue() != null && snapshotDetails.getValue().equalsIgnoreCase("create")) {
            long sfAccountId = getCreateSolidFireAccountId(sfConnection, snapshotInfo.getAccountId(), storagePoolId);

            SolidFireUtil.SolidFireVolume sfVolume = createCloneFromSnapshot(sfConnection, csSnapshotId, sfAccountId);

            addTempVolumeId(csSnapshotId, String.valueOf(sfVolume.getId()));

            handleSnapshotDetails(csSnapshotId, DiskTO.IQN, sfVolume.getIqn());
        }
        else if (snapshotDetails != null && snapshotDetails.getValue() != null && snapshotDetails.getValue().equalsIgnoreCase("delete")) {
            snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

            SolidFireUtil.deleteSolidFireVolume(sfConnection, Long.parseLong(snapshotDetails.getValue()));

            removeTempVolumeId(csSnapshotId);

            snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, DiskTO.IQN);

            _snapshotDetailsDao.remove(snapshotDetails.getId());
        }
        else {
            throw new CloudRuntimeException("Invalid state in 'createTempVolume(SnapshotInfo, long)'");
        }
    }

    private void handleSnapshotDetails(long csSnapshotId, String name, String value) {
        _snapshotDetailsDao.removeDetail(csSnapshotId, name);

        SnapshotDetailsVO snapshotDetails = new SnapshotDetailsVO(csSnapshotId, name, value, false);

        _snapshotDetailsDao.persist(snapshotDetails);
    }

    private void addTempVolumeId(long csSnapshotId, String tempVolumeId) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

        if (snapshotDetails == null || snapshotDetails.getValue() == null) {
            throw new CloudRuntimeException("'addTempVolumeId' should not be invoked unless " + SolidFireUtil.VOLUME_ID + " exists.");
        }

        String originalVolumeId = snapshotDetails.getValue();

        handleSnapshotDetails(csSnapshotId, SolidFireUtil.TEMP_VOLUME_ID, originalVolumeId);
        handleSnapshotDetails(csSnapshotId, SolidFireUtil.VOLUME_ID, tempVolumeId);
    }

    private void removeTempVolumeId(long csSnapshotId) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.TEMP_VOLUME_ID);

        if (snapshotDetails == null || snapshotDetails.getValue() == null) {
            throw new CloudRuntimeException("'removeTempVolumeId' should not be invoked unless " + SolidFireUtil.TEMP_VOLUME_ID + " exists.");
        }

        String originalVolumeId = snapshotDetails.getValue();

        handleSnapshotDetails(csSnapshotId, SolidFireUtil.VOLUME_ID, originalVolumeId);

        _snapshotDetailsDao.remove(snapshotDetails.getId());
    }

    private SolidFireUtil.SolidFireVolume createCloneFromSnapshot(SolidFireUtil.SolidFireConnection sfConnection, long csSnapshotId, long sfAccountId) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

        long sfVolumeId = Long.parseLong(snapshotDetails.getValue());

        snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

        long sfSnapshotId = Long.parseLong(snapshotDetails.getValue());

        SolidFireUtil.SolidFireSnapshot sfSnapshot = SolidFireUtil.getSolidFireSnapshot(sfConnection, sfVolumeId, sfSnapshotId);

        long newSfVolumeId = SolidFireUtil.createSolidFireClone(sfConnection, sfVolumeId, sfSnapshotId, sfAccountId, sfSnapshot.getName(), null);

        snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.STORAGE_POOL_ID);

        long storagePoolId = Long.parseLong(snapshotDetails.getValue());

        final Iops iops = getIops(MIN_IOPS_FOR_TEMP_VOLUME, MAX_IOPS_FOR_TEMP_VOLUME, storagePoolId);

        SolidFireUtil.modifySolidFireVolume(sfConnection, newSfVolumeId, null, null, iops.getMinIops(), iops.getMaxIops(), iops.getBurstIops());

        return SolidFireUtil.getSolidFireVolume(sfConnection, newSfVolumeId);
    }

    private String createTemplateVolume(TemplateInfo templateInfo, long storagePoolId) {
        verifySufficientBytesForStoragePool(templateInfo, storagePoolId);

        SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

        long sfAccountId = getCreateSolidFireAccountId(sfConnection, templateInfo.getAccountId(), storagePoolId);

        SolidFireUtil.SolidFireVolume sfVolume = createSolidFireVolume(sfConnection, templateInfo, storagePoolId, sfAccountId);

        String iqn = sfVolume.getIqn();

        VMTemplateStoragePoolVO templatePoolRef = _tmpltPoolDao.findByPoolTemplate(storagePoolId, templateInfo.getId());

        templatePoolRef.setInstallPath(iqn);
        templatePoolRef.setLocalDownloadPath(Long.toString(sfVolume.getId()));
        templatePoolRef.setTemplateSize(sfVolume.getTotalSize());

        _tmpltPoolDao.update(templatePoolRef.getId(), templatePoolRef);

        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

        long capacityBytes = storagePool.getCapacityBytes();
        // getUsedBytes(StoragePool) will include the bytes of the newly created template volume because
        // _tmpltPoolDao.update(Long, VMTemplateStoragePoolVO) has already been invoked
        long usedBytes = getUsedBytes(storagePool);

        storagePool.setUsedBytes(usedBytes > capacityBytes ? capacityBytes : usedBytes);

        _storagePoolDao.update(storagePoolId, storagePool);

        return iqn;
    }

    private long getCreateSolidFireAccountId(SolidFireUtil.SolidFireConnection sfConnection, long csAccountId, long storagePoolId) {
        AccountDetailVO accountDetail = SolidFireUtil.getAccountDetail(csAccountId, storagePoolId, _accountDetailsDao);

        if (accountDetail == null || accountDetail.getValue() == null) {
            AccountVO account = _accountDao.findById(csAccountId);
            String sfAccountName = SolidFireUtil.getSolidFireAccountName(account.getUuid(), account.getAccountId());
            SolidFireUtil.SolidFireAccount sfAccount = SolidFireUtil.getSolidFireAccount(sfConnection, sfAccountName);

            if (sfAccount == null) {
                sfAccount = createSolidFireAccount(sfConnection, sfAccountName);
            }

            SolidFireUtil.updateCsDbWithSolidFireAccountInfo(account.getId(), sfAccount, storagePoolId, _accountDetailsDao);

            accountDetail = SolidFireUtil.getAccountDetail(csAccountId, storagePoolId, _accountDetailsDao);
        }

        return Long.parseLong(accountDetail.getValue());
    }

    @Override
    public long getUsedIops(StoragePool storagePool) {
        long usedIops = 0;

        List<VolumeVO> volumes = _volumeDao.findByPoolId(storagePool.getId(), null);

        if (volumes != null) {
            for (VolumeVO volume : volumes) {
                usedIops += volume.getMinIops() != null ? volume.getMinIops() : 0;
            }
        }

        return usedIops;
    }

    private void verifySufficientBytesForStoragePool(long requestedBytes, long storagePoolId) {
        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

        long capacityBytes = storagePool.getCapacityBytes();
        long usedBytes = getUsedBytes(storagePool);

        usedBytes += requestedBytes;

        if (usedBytes > capacityBytes) {
            throw new CloudRuntimeException("Insufficient amount of space remains in this primary storage");
        }
    }

    private void verifySufficientBytesForStoragePool(DataObject dataObject, long storagePoolId) {
        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

        long requestedBytes = getDataObjectSizeIncludingHypervisorSnapshotReserve(dataObject, storagePool);

        verifySufficientBytesForStoragePool(requestedBytes, storagePoolId);
    }

    private void verifySufficientBytesForStoragePool(long storagePoolId, long volumeId, long newSize, Integer newHypervisorSnapshotReserve) {
        DataStore primaryDataStore = _dataStoreMgr.getDataStore(storagePoolId, DataStoreRole.Primary);
        VolumeInfo volumeInfo = _volumeFactory.getVolume(volumeId, primaryDataStore);
        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);
        long currentSizeWithHsr = getDataObjectSizeIncludingHypervisorSnapshotReserve(volumeInfo, storagePool);

        newHypervisorSnapshotReserve = newHypervisorSnapshotReserve == null ? LOWEST_HYPERVISOR_SNAPSHOT_RESERVE :
                Math.max(newHypervisorSnapshotReserve, LOWEST_HYPERVISOR_SNAPSHOT_RESERVE);

        long newSizeWithHsr = (long)(newSize + newSize * (newHypervisorSnapshotReserve / 100f));

        if (newSizeWithHsr < currentSizeWithHsr) {
            throw new CloudRuntimeException("Storage pool " + storagePoolId + " does not support shrinking a volume.");
        }

        long availableBytes = storagePool.getCapacityBytes() - getUsedBytes(storagePool);

        if ((newSizeWithHsr - currentSizeWithHsr) > availableBytes) {
            throw new CloudRuntimeException("Storage pool " + storagePoolId + " does not have enough space to expand the volume.");
        }
    }

    private void verifySufficientIopsForStoragePool(long requestedIops, long storagePoolId) {
        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

        long usedIops = getUsedIops(storagePool);
        long capacityIops = storagePool.getCapacityIops();

        if (usedIops + requestedIops > capacityIops) {
            throw new CloudRuntimeException("Insufficient number of IOPS available in this storage pool");
        }
    }

    private void verifySufficientIopsForStoragePool(long storagePoolId, long volumeId, long newMinIops) {
        VolumeVO volume = _volumeDao.findById(volumeId);

        long currentMinIops = volume.getMinIops() != null ? volume.getMinIops() : 0;
        long diffInMinIops = newMinIops - currentMinIops;

        // if the desire is for more IOPS
        if (diffInMinIops > 0) {
            verifySufficientIopsForStoragePool(diffInMinIops, storagePoolId);
        }
    }

    private SolidFireUtil.SolidFireVolume createClone(SolidFireUtil.SolidFireConnection sfConnection, long dataObjectId, VolumeInfo volumeInfo, long sfAccountId,
                                                      long storagePoolId, DataObjectType dataObjectType) {
        String sfNewVolumeName = volumeInfo.getName();

        long sfVolumeId = Long.MIN_VALUE;
        long sfSnapshotId = Long.MIN_VALUE;

        if (dataObjectType == DataObjectType.SNAPSHOT) {
            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(dataObjectId, SolidFireUtil.SNAPSHOT_ID);

            if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                sfSnapshotId = Long.parseLong(snapshotDetails.getValue());
            }

            snapshotDetails = _snapshotDetailsDao.findDetail(dataObjectId, SolidFireUtil.VOLUME_ID);

            sfVolumeId = Long.parseLong(snapshotDetails.getValue());
        } else if (dataObjectType == DataObjectType.TEMPLATE) {
            // get the cached template on this storage
            VMTemplateStoragePoolVO templatePoolRef = _tmpltPoolDao.findByPoolTemplate(storagePoolId, dataObjectId);

            if (templatePoolRef != null) {
                sfVolumeId = Long.parseLong(templatePoolRef.getLocalDownloadPath());
            }
        }

        if (sfVolumeId <= 0) {
            throw new CloudRuntimeException("Unable to find SolidFire volume for the following data-object ID: " + dataObjectId +
                    " and data-object type: " + dataObjectType);
        }

        final long newSfVolumeId = SolidFireUtil.createSolidFireClone(sfConnection, sfVolumeId, sfSnapshotId, sfAccountId, sfNewVolumeName,
                getVolumeAttributes(volumeInfo));

        final Iops iops = getIops(volumeInfo.getMinIops(), volumeInfo.getMaxIops(), storagePoolId);

        SolidFireUtil.modifySolidFireVolume(sfConnection, newSfVolumeId, null, null, iops.getMinIops(), iops.getMaxIops(), iops.getBurstIops());

        return SolidFireUtil.getSolidFireVolume(sfConnection, newSfVolumeId);
    }

    private Iops getIops(Long minIops, Long maxIops, long storagePoolId) {
        if (minIops == null || minIops <= 0 || maxIops == null || maxIops <= 0) {
            long defaultMaxIops = getDefaultMaxIops(storagePoolId);

            return new Iops(getDefaultMinIops(storagePoolId), defaultMaxIops, getDefaultBurstIops(storagePoolId, defaultMaxIops));
        }

        return new Iops(minIops, maxIops, getDefaultBurstIops(storagePoolId, maxIops));
    }

    private long getCsIdForCloning(long volumeId, String cloneOf) {
        VolumeDetailVO volumeDetail = _volumeDetailsDao.findDetail(volumeId, cloneOf);

        if (volumeDetail != null && volumeDetail.getValue() != null) {
            return new Long(volumeDetail.getValue());
        }

        return Long.MIN_VALUE;
    }

    private Map<String, String> getVolumeAttributes(VolumeInfo volumeInfo) {
        Map<String, String> mapAttributes = new HashMap<>();

        mapAttributes.put(SolidFireUtil.CloudStackVolumeId, String.valueOf(volumeInfo.getId()));
        mapAttributes.put(SolidFireUtil.CloudStackVolumeSize, NumberFormat.getInstance().format(volumeInfo.getSize()));

        return mapAttributes;
    }

    private Map<String, String> getSnapshotAttributes(SnapshotInfo snapshotInfo) {
        Map<String, String> mapAttributes = new HashMap<>();

        mapAttributes.put(SolidFireUtil.CloudStackSnapshotId, String.valueOf(snapshotInfo.getId()));
        mapAttributes.put(SolidFireUtil.CloudStackSnapshotSize, NumberFormat.getInstance().format(snapshotInfo.getSize()));

        return mapAttributes;
    }

    private Map<String, String> getTemplateAttributes(TemplateInfo templateInfo) {
        Map<String, String> mapAttributes = new HashMap<>();

        mapAttributes.put(SolidFireUtil.CloudStackTemplateId, String.valueOf(templateInfo.getId()));
        mapAttributes.put(SolidFireUtil.CloudStackTemplateSize, NumberFormat.getInstance().format(templateInfo.getSize()));

        return mapAttributes;
    }

    private void updateVolumeDetails(long volumeId, long sfVolumeSize) {
        _volumeDetailsDao.removeDetail(volumeId, SolidFireUtil.VOLUME_SIZE);

        VolumeDetailVO volumeDetailVo = new VolumeDetailVO(volumeId, SolidFireUtil.VOLUME_SIZE, String.valueOf(sfVolumeSize), false);

        _volumeDetailsDao.persist(volumeDetailVo);
    }

    private void updateSnapshotDetails(long csSnapshotId, long csVolumeId, long sfVolumeId, long sfNewSnapshotId, long storagePoolId, long sfNewVolumeSize) {
        SnapshotDetailsVO snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.ORIG_CS_VOLUME_ID,
                String.valueOf(csVolumeId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.VOLUME_ID,
                String.valueOf(sfVolumeId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.SNAPSHOT_ID,
                String.valueOf(sfNewSnapshotId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.STORAGE_POOL_ID,
                String.valueOf(storagePoolId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.VOLUME_SIZE,
                String.valueOf(sfNewVolumeSize),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);
    }

    private void updateSnapshotDetails(long csSnapshotId, long sfNewVolumeId, long storagePoolId, long sfNewVolumeSize, String sfNewVolumeIqn) {
        SnapshotDetailsVO snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.VOLUME_ID,
                String.valueOf(sfNewVolumeId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.STORAGE_POOL_ID,
                String.valueOf(storagePoolId),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                SolidFireUtil.VOLUME_SIZE,
                String.valueOf(sfNewVolumeSize),
                false);

        _snapshotDetailsDao.persist(snapshotDetail);

        snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
                DiskTO.IQN,
                sfNewVolumeIqn,
                false);

        _snapshotDetailsDao.persist(snapshotDetail);
    }

    @Override
    public void deleteAsync(DataStore dataStore, DataObject dataObject, AsyncCompletionCallback<CommandResult> callback) {
        String errMsg = null;

        try {
            if (dataObject.getType() == DataObjectType.VOLUME) {
                deleteVolume((VolumeInfo)dataObject, dataStore.getId());
            } else if (dataObject.getType() == DataObjectType.SNAPSHOT) {
                deleteSnapshot((SnapshotInfo)dataObject, dataStore.getId());
            } else if (dataObject.getType() == DataObjectType.TEMPLATE) {
                deleteTemplate((TemplateInfo)dataObject, dataStore.getId());
            } else {
                errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to deleteAsync";
            }
        }
        catch (Exception ex) {
            errMsg = ex.getMessage();

            s_logger.error(errMsg);
        }

        if (callback != null) {
            CommandResult result = new CommandResult();

            result.setResult(errMsg);

            callback.complete(result);
        }
    }

    private void deleteVolume(VolumeInfo volumeInfo, long storagePoolId) {
        try {
            long volumeId = volumeInfo.getId();

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            deleteSolidFireVolume(sfConnection, volumeInfo);

            _volumeDetailsDao.removeDetails(volumeId);

            StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

            long usedBytes = getUsedBytes(storagePool, volumeId);

            storagePool.setUsedBytes(usedBytes < 0 ? 0 : usedBytes);

            _storagePoolDao.update(storagePoolId, storagePool);
        }
        catch (Exception ex) {
            s_logger.debug(SolidFireUtil.LOG_PREFIX + "Failed to delete SolidFire volume. CloudStack volume ID: " + volumeInfo.getId(), ex);

            throw ex;
        }
    }

    private void deleteSnapshot(SnapshotInfo snapshotInfo, long storagePoolId) {
        long csSnapshotId = snapshotInfo.getId();

        try {
            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

            if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                // A SolidFire snapshot is being used to support the CloudStack volume snapshot.

                long sfSnapshotId = Long.parseLong(snapshotDetails.getValue());

                deleteSolidFireSnapshot(sfConnection, csSnapshotId, sfSnapshotId);
            }
            else {
                // A SolidFire volume is being used to support the CloudStack volume snapshot.

                snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

                long sfVolumeId = Long.parseLong(snapshotDetails.getValue());

                SolidFireUtil.deleteSolidFireVolume(sfConnection, sfVolumeId);
            }

            _snapshotDetailsDao.removeDetails(csSnapshotId);

            StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

            // getUsedBytes(StoragePool) will not include the snapshot to delete because it has already been deleted by this point
            long usedBytes = getUsedBytes(storagePool);

            storagePool.setUsedBytes(usedBytes < 0 ? 0 : usedBytes);

            _storagePoolDao.update(storagePoolId, storagePool);
        }
        catch (Exception ex) {
            s_logger.debug(SolidFireUtil.LOG_PREFIX + "Issue in 'deleteSnapshot(SnapshotInfo, long)'. CloudStack snapshot ID: " + csSnapshotId, ex);

            throw ex;
        }
    }

    private void deleteSolidFireSnapshot(SolidFireUtil.SolidFireConnection sfConnection, long csSnapshotId, long sfSnapshotId) {
        SolidFireUtil.deleteSolidFireSnapshot(sfConnection, sfSnapshotId);

        final VolumeVO volume;
        final long volumeId;

        SnapshotVO snapshot = _snapshotDao.findById(csSnapshotId);
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.ORIG_CS_VOLUME_ID);

        if (snapshotDetails != null && snapshotDetails.getValue() != null) {
            volumeId = Long.valueOf(snapshotDetails.getValue());

            volume = _volumeDao.findById(volumeId);
        }
        else {
            volumeId = snapshot.getVolumeId();

            volume = _volumeDao.findById(volumeId);
        }

        if (volume == null) { // if the CloudStack volume has been deleted
            List<SnapshotVO> lstSnapshots = getNonDestroyedSnapshots(snapshot.getVolumeId());

            List<SnapshotVO> lstSnapshots2 = new ArrayList<>();

            for (SnapshotVO snapshotVo : lstSnapshots) {
                // The CloudStack volume snapshot has not yet been set to the DESTROYED state, so check to make
                // sure snapshotVo.getId() != csSnapshotId when determining if any volume snapshots remain for the given CloudStack volume.
                if (snapshotVo.getId() != csSnapshotId) {
                    snapshotDetails = _snapshotDetailsDao.findDetail(snapshotVo.getId(), SolidFireUtil.SNAPSHOT_ID);

                    // We are only interested here in volume snapshots that make use of SolidFire snapshots (as opposed to ones
                    // that make use of SolidFire volumes).
                    if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                        lstSnapshots2.add(snapshotVo);
                    }
                }
            }

            if (lstSnapshots2.isEmpty()) {
                VolumeVO volumeToDelete = _volumeDao.findByIdIncludingRemoved(volumeId);

                SolidFireUtil.deleteSolidFireVolume(sfConnection, Long.parseLong(volumeToDelete.getFolder()));
            }
        }
    }

    private List<SnapshotVO> getNonDestroyedSnapshots(long csVolumeId) {
        List<SnapshotVO> lstSnapshots = _snapshotDao.listByVolumeId(csVolumeId);

        if (lstSnapshots == null) {
            lstSnapshots = new ArrayList<>();
        }

        List<SnapshotVO> lstSnapshots2 = new ArrayList<>();

        for (SnapshotVO snapshot : lstSnapshots) {
            if (!State.Destroyed.equals(snapshot.getState())) {
                lstSnapshots2.add(snapshot);
            }
        }

        return lstSnapshots2;
    }

    private void deleteTemplate(TemplateInfo template, long storagePoolId) {
        try {
            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            long sfTemplateVolumeId = getVolumeIdFrom_iScsiPath(template.getInstallPath());

            SolidFireUtil.deleteSolidFireVolume(sfConnection, sfTemplateVolumeId);

            StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

            // getUsedBytes(StoragePool) will not include the template to delete because the "template_spool_ref" table has already been updated by this point
            long usedBytes = getUsedBytes(storagePool);

            storagePool.setUsedBytes(usedBytes < 0 ? 0 : usedBytes);

            _storagePoolDao.update(storagePoolId, storagePool);
        }
        catch (Exception ex) {
            s_logger.debug(SolidFireUtil.LOG_PREFIX + "Failed to delete SolidFire template volume. CloudStack template ID: " + template.getId(), ex);

            throw ex;
        }
    }

    @Override
    public void copyAsync(DataObject srcData, DataObject destData, AsyncCompletionCallback<CopyCommandResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canCopy(DataObject srcData, DataObject destData) {
        return false;
    }

    private boolean shouldTakeSnapshot(long snapshotId) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotId, "takeSnapshot");

        if (snapshotDetails != null && snapshotDetails.getValue() != null) {
            return Boolean.parseBoolean(snapshotDetails.getValue());
        }

        return false;
    }

    @Override
    public void takeSnapshot(SnapshotInfo snapshotInfo, AsyncCompletionCallback<CreateCmdResult> callback) {
        CreateCmdResult result;

        try {
            VolumeInfo volumeInfo = snapshotInfo.getBaseVolume();
            VolumeVO volumeVO = _volumeDao.findById(volumeInfo.getId());

            long sfVolumeId = Long.parseLong(volumeVO.getFolder());
            long storagePoolId = volumeVO.getPoolId();

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            SolidFireUtil.SolidFireVolume sfVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfVolumeId);

            StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

            long capacityBytes = storagePool.getCapacityBytes();

            // getUsedBytes(StoragePool) will not include the bytes of the proposed new volume or snapshot because
            // updateSnapshotDetails has not yet been called for this new volume or snapshot
            long usedBytes = getUsedBytes(storagePool);
            long sfVolumeSize = sfVolume.getTotalSize();

            usedBytes += sfVolumeSize;

            // For creating a volume or a snapshot, we need to check to make sure a sufficient amount of space remains in the primary storage.
            // For the purpose of "charging" these bytes against storage_pool.capacity_bytes, we take the full size of the SolidFire volume
            // that is serving as the volume the snapshot is of (either a new SolidFire volume or a SolidFire snapshot).
            if (usedBytes > capacityBytes) {
                throw new CloudRuntimeException("Insufficient amount of space remains in this primary storage to take a snapshot");
            }

            storagePool.setUsedBytes(usedBytes);

            SnapshotObjectTO snapshotObjectTo = (SnapshotObjectTO)snapshotInfo.getTO();

            if (shouldTakeSnapshot(snapshotInfo.getId())) {
                // We are supposed to take a SolidFire snapshot to serve as the back-end for our CloudStack volume snapshot.

                String sfNewSnapshotName = volumeInfo.getName() + "-" + snapshotInfo.getUuid();

                long sfNewSnapshotId = SolidFireUtil.createSolidFireSnapshot(sfConnection, sfVolumeId, sfNewSnapshotName, getSnapshotAttributes(snapshotInfo));

                updateSnapshotDetails(snapshotInfo.getId(), volumeInfo.getId(), sfVolumeId, sfNewSnapshotId, storagePoolId, sfVolumeSize);

                snapshotObjectTo.setPath("SfSnapshotId=" + sfNewSnapshotId);
            }
            else {
                // We are supposed to create a new SolidFire volume to serve as the back-end for our CloudStack volume snapshot.

                String sfNewVolumeName = volumeInfo.getName() + "-" + snapshotInfo.getUuid();

                final Iops iops = getIops(MIN_IOPS_FOR_SNAPSHOT_VOLUME, MAX_IOPS_FOR_SNAPSHOT_VOLUME, storagePoolId);

                long sfNewVolumeId = SolidFireUtil.createSolidFireVolume(sfConnection, sfNewVolumeName, sfVolume.getAccountId(), sfVolumeSize,
                        sfVolume.isEnable512e(), getSnapshotAttributes(snapshotInfo), iops.getMinIops(), iops.getMaxIops(), iops.getBurstIops());

                SolidFireUtil.SolidFireVolume sfNewVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfNewVolumeId);

                updateSnapshotDetails(snapshotInfo.getId(), sfNewVolumeId, storagePoolId, sfVolumeSize, sfNewVolume.getIqn());

                snapshotObjectTo.setPath("SfVolumeId=" + sfNewVolumeId);
            }

            // Now that we have successfully created a volume or a snapshot, update the space usage in the cloud.storage_pool table
            // (even though cloud.storage_pool.used_bytes is likely no longer in use).
            _storagePoolDao.update(storagePoolId, storagePool);

            CreateObjectAnswer createObjectAnswer = new CreateObjectAnswer(snapshotObjectTo);

            result = new CreateCmdResult(null, createObjectAnswer);

            result.setResult(null);
        }
        catch (Exception ex) {
            s_logger.debug(SolidFireUtil.LOG_PREFIX + "Failed to take CloudStack snapshot: " + snapshotInfo.getId(), ex);

            result = new CreateCmdResult(null, new CreateObjectAnswer(ex.toString()));

            result.setResult(ex.toString());
        }

        callback.complete(result);
    }

    @Override
    public void revertSnapshot(SnapshotInfo snapshot, SnapshotInfo snapshot2, AsyncCompletionCallback<CommandResult> callback) {
        VolumeInfo volumeInfo = snapshot.getBaseVolume();

        VolumeVO volumeVO = _volumeDao.findById(volumeInfo.getId());

        if (volumeVO == null || volumeVO.getRemoved() != null) {
            String errMsg = "The volume that the snapshot belongs to no longer exists.";

            CommandResult commandResult = new CommandResult();

            commandResult.setResult(errMsg);

            callback.complete(commandResult);

            return;
        }

        SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(volumeVO.getPoolId(), _storagePoolDetailsDao);

        long sfVolumeId = Long.parseLong(volumeInfo.getFolder());

        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshot.getId(), SolidFireUtil.SNAPSHOT_ID);

        long sfSnapshotId = Long.parseLong(snapshotDetails.getValue());

        SolidFireUtil.SolidFireVolume sfVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfVolumeId);

        SolidFireUtil.SolidFireSnapshot sfSnapshot = SolidFireUtil.getSolidFireSnapshot(sfConnection, sfVolumeId, sfSnapshotId);

        if (sfVolume.getTotalSize() != sfSnapshot.getTotalSize()) {
            // although the SolidFire system is capable of performing this kind of a revert, we want to prohibit it in this case in CloudStack
            String errMsg = "The volume size is different from the snapshot size. Recover the data on the snapshot by creating " +
                    "a new CloudStack volume from the corresponding volume snapshot.";

            CommandResult commandResult = new CommandResult();

            commandResult.setResult(errMsg);

            callback.complete(commandResult);

            return;
        }

        SolidFireUtil.rollBackVolumeToSnapshot(sfConnection, sfVolumeId, sfSnapshotId);

        CommandResult commandResult = new CommandResult();

        callback.complete(commandResult);
    }

    @Override
    public void resize(DataObject dataObject, AsyncCompletionCallback<CreateCmdResult> callback) {
        String iqn = null;
        String errMsg = null;

        try {
            if (dataObject.getType() == DataObjectType.VOLUME) {
                VolumeInfo volumeInfo = (VolumeInfo)dataObject;
                iqn = volumeInfo.get_iScsiName();
                long storagePoolId = volumeInfo.getPoolId();
                long sfVolumeId = Long.parseLong(volumeInfo.getFolder());
                ResizeVolumePayload payload = (ResizeVolumePayload)volumeInfo.getpayload();

                SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);
                SolidFireUtil.SolidFireVolume sfVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfVolumeId);

                long newMinIops = payload.newMinIops != null ? payload.newMinIops : sfVolume.getMinIops();

                verifySufficientIopsForStoragePool(storagePoolId, volumeInfo.getId(), newMinIops);

                long newSize = payload.newSize != null ? payload.newSize : volumeInfo.getSize();
                Integer hsr = volumeInfo.getHypervisorSnapshotReserve();

                verifySufficientBytesForStoragePool(storagePoolId, volumeInfo.getId(), newSize, hsr);

                long sfNewVolumeSize = sfVolume.getTotalSize();

                if (payload.newSize != null) {
                    sfNewVolumeSize = getVolumeSizeIncludingHypervisorSnapshotReserve(newSize, hsr);
                }

                Map<String, String> mapAttributes = new HashMap<>();

                mapAttributes.put(SolidFireUtil.CloudStackVolumeId, String.valueOf(volumeInfo.getId()));
                mapAttributes.put(SolidFireUtil.CloudStackVolumeSize, NumberFormat.getInstance().format(payload.newSize));

                long newMaxIops = payload.newMaxIops != null ? payload.newMaxIops : sfVolume.getMaxIops();

                SolidFireUtil.modifySolidFireVolume(sfConnection, sfVolumeId, sfNewVolumeSize, mapAttributes,
                        newMinIops, newMaxIops, getDefaultBurstIops(storagePoolId, newMaxIops));

                VolumeVO volume = _volumeDao.findById(volumeInfo.getId());

                volume.setMinIops(newMinIops);
                volume.setMaxIops(newMaxIops);

                _volumeDao.update(volume.getId(), volume);

                // SolidFireUtil.VOLUME_SIZE was introduced in 4.5.
                updateVolumeDetails(volume.getId(), sfNewVolumeSize);
            } else {
                errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to resize";
            }
        }
        catch (Exception ex) {
            errMsg = ex.getMessage();
        }
        finally {
            CreateCmdResult result = new CreateCmdResult(iqn, new Answer(null, errMsg == null, errMsg));

            result.setResult(errMsg);

            callback.complete(result);
        }
    }
}
