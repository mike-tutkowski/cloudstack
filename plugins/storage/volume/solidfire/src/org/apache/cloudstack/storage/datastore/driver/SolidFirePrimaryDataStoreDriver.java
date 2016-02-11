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
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStoreDriver;
import org.apache.cloudstack.engine.subsystem.api.storage.SnapshotInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.storage.command.CommandResult;
import org.apache.cloudstack.storage.command.CreateObjectAnswer;
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
import com.cloud.storage.Snapshot.State;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.ResizeVolumePayload;
import com.cloud.storage.StoragePool;
import com.cloud.storage.Volume;
import com.cloud.storage.VolumeDetailVO;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.SnapshotVO;
import com.cloud.storage.dao.SnapshotDao;
import com.cloud.storage.dao.SnapshotDetailsDao;
import com.cloud.storage.dao.SnapshotDetailsVO;
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
    private static final int s_lockTimeInSeconds = 300;
    private static final int s_lowestHypervisorSnapshotReserve = 10;

    @Inject private AccountDao _accountDao;
    @Inject private AccountDetailsDao _accountDetailsDao;
    @Inject private ClusterDao _clusterDao;
    @Inject private ClusterDetailsDao _clusterDetailsDao;
    @Inject private HostDao _hostDao;
    @Inject private SnapshotDao _snapshotDao;
    @Inject private SnapshotDetailsDao _snapshotDetailsDao;
    @Inject private PrimaryDataStoreDao _storagePoolDao;
    @Inject private StoragePoolDetailsDao _storagePoolDetailsDao;
    @Inject private VolumeDao _volumeDao;
    @Inject private VolumeDetailsDao _volumeDetailsDao;

    @Override
    public Map<String, String> getCapabilities() {
        Map<String, String> mapCapabilities = new HashMap<String, String>();

        mapCapabilities.put(DataStoreCapabilities.STORAGE_SYSTEM_SNAPSHOT.toString(), Boolean.TRUE.toString());
        mapCapabilities.put(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_SNAPSHOT.toString(), Boolean.TRUE.toString());
        mapCapabilities.put(DataStoreCapabilities.CAN_CREATE_VOLUME_FROM_VOLUME.toString(), Boolean.TRUE.toString());

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
    public ChapInfo getChapInfo(VolumeInfo volumeInfo) {
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

        if (!lock.lock(s_lockTimeInSeconds)) {
            s_logger.debug("Couldn't lock the DB (in grantAccess) on the following string: " + cluster.getUuid());
        }

        try {
            ClusterDetailsVO clusterDetail = _clusterDetailsDao.findDetail(clusterId, SolidFireUtil.getVagKey(storagePoolId));

            String vagId = clusterDetail != null ? clusterDetail.getValue() : null;

            List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

            if (!SolidFireUtil.hostsSupport_iScsi(hosts)) {
                return false;
            }

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            if (vagId != null) {
                SolidFireUtil.SolidFireVag sfVag = SolidFireUtil.getSolidFireVag(sfConnection, Long.parseLong(vagId));

                String[] hostIqns = SolidFireUtil.getNewHostIqns(sfVag.getInitiators(), SolidFireUtil.getIqnsFromHosts(hosts));
                long[] volumeIds = SolidFireUtil.getNewVolumeIds(sfVag.getVolumeIds(), sfVolumeId, true);

                SolidFireUtil.modifySolidFireVag(sfConnection, sfVag.getId(), hostIqns, volumeIds);
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

        if (!lock.lock(s_lockTimeInSeconds)) {
            s_logger.debug("Couldn't lock the DB (in revokeAccess) on the following string: " + cluster.getUuid());
        }

        try {
            ClusterDetailsVO clusterDetail = _clusterDetailsDao.findDetail(clusterId, SolidFireUtil.getVagKey(storagePoolId));

            String vagId = clusterDetail != null ? clusterDetail.getValue() : null;

            if (vagId != null) {
                List<HostVO> hosts = _hostDao.findByClusterId(clusterId);

                SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

                SolidFireUtil.SolidFireVag sfVag = SolidFireUtil.getSolidFireVag(sfConnection, Long.parseLong(vagId));

                String[] hostIqns = SolidFireUtil.getNewHostIqns(sfVag.getInitiators(), SolidFireUtil.getIqnsFromHosts(hosts));
                long[] volumeIds = SolidFireUtil.getNewVolumeIds(sfVag.getVolumeIds(), sfVolumeId, false);

                SolidFireUtil.modifySolidFireVag(sfConnection, sfVag.getId(), hostIqns, volumeIds);
            }
        }
        finally {
            lock.unlock();
            lock.releaseRef();
        }
    }

    private long getSolidFireVolumeId(DataObject dataObject) {
        if (dataObject.getType() == DataObjectType.VOLUME) {
            return Long.parseLong(((VolumeInfo)dataObject).getFolder());
        }

        if (dataObject.getType() == DataObjectType.SNAPSHOT) {
            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(dataObject.getId(), SolidFireUtil.VOLUME_ID);

            if (snapshotDetails == null || snapshotDetails.getValue() == null) {
                throw new CloudRuntimeException("Unable to locate the volume ID associated with the following snapshot ID: " + dataObject.getId());
            }

            return Long.parseLong(snapshotDetails.getValue());
        }

        throw new CloudRuntimeException("Invalid DataObjectType (" + dataObject.getType() + ") passed to getSolidFireVolumeId(DataObject)");
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

        return (long)(maxIops * fClusterDefaultBurstIopsPercentOfMaxIops);
    }

    private SolidFireUtil.SolidFireVolume createSolidFireVolume(SolidFireUtil.SolidFireConnection sfConnection, VolumeInfo volumeInfo, long sfAccountId) {
        long storagePoolId = volumeInfo.getDataStore().getId();

        final Iops iops;

        Long minIops = volumeInfo.getMinIops();
        Long maxIops = volumeInfo.getMaxIops();

        if (minIops == null || minIops <= 0 || maxIops == null || maxIops <= 0) {
            long defaultMaxIops = getDefaultMaxIops(storagePoolId);

            iops = new Iops(getDefaultMinIops(storagePoolId), defaultMaxIops, getDefaultBurstIops(storagePoolId, defaultMaxIops));
        } else {
            iops = new Iops(volumeInfo.getMinIops(), volumeInfo.getMaxIops(), getDefaultBurstIops(storagePoolId, volumeInfo.getMaxIops()));
        }

        long volumeSize = getVolumeSizeIncludingHypervisorSnapshotReserve(volumeInfo);

        long sfVolumeId = SolidFireUtil.createSolidFireVolume(sfConnection, SolidFireUtil.getSolidFireVolumeName(volumeInfo.getName()), sfAccountId,
                volumeSize, true, getVolumeAttributes(volumeInfo), iops.getMinIops(), iops.getMaxIops(), iops.getBurstIops());

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

        return usedSpace;
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

    public long getVolumeSizeIncludingHypervisorSnapshotReserve(long volumeSize, Integer hypervisorSnapshotReserve) {

        if (hypervisorSnapshotReserve != null) {
            hypervisorSnapshotReserve = Math.max(hypervisorSnapshotReserve, s_lowestHypervisorSnapshotReserve);

            volumeSize += volumeSize * (hypervisorSnapshotReserve / 100f);
        }

        return volumeSize;
    }

    @Override
    public long getVolumeSizeIncludingHypervisorSnapshotReserve(Volume volume) {
        long volumeSize = volume.getSize();
        Integer hypervisorSnapshotReserve = volume.getHypervisorSnapshotReserve();

        if (hypervisorSnapshotReserve != null) {
            hypervisorSnapshotReserve = Math.max(hypervisorSnapshotReserve, s_lowestHypervisorSnapshotReserve);

            volumeSize += volumeSize * (hypervisorSnapshotReserve / 100f);
        }

        return volumeSize;
    }

    private static class Iops {
        private final long _minIops;
        private final long _maxIops;
        private final long _burstIops;

        public Iops(long minIops, long maxIops, long burstIops) throws IllegalArgumentException {
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

        public long getMinIops() {
            return _minIops;
        }

        public long getMaxIops() {
            return _maxIops;
        }

        public long getBurstIops() {
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

    @Override
    public void createAsync(DataStore dataStore, DataObject dataObject, AsyncCompletionCallback<CreateCmdResult> callback) {
        String iqn = null;
        String errMsg = null;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            VolumeInfo volumeInfo = (VolumeInfo)dataObject;

            long storagePoolId = dataStore.getId();

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            long csSnapshotId = getCsSnapshotIdForCloning(volumeInfo.getId());

            SolidFireUtil.SolidFireVolume sfVolume = null;

            // if csSnapshotId > 0, then we are supposed to create a clone of the underlying volume or snapshot that supports the CloudStack snapshot
            if (csSnapshotId > 0) {
                sfVolume = createClone(sfConnection, csSnapshotId, volumeInfo);
            }
            else {
                AccountDetailVO accountDetail = SolidFireUtil.getAccountDetail(volumeInfo.getAccountId(), storagePoolId, _accountDetailsDao);

                if (accountDetail == null || accountDetail.getValue() == null) {
                    AccountVO account = _accountDao.findById(volumeInfo.getAccountId());
                    String sfAccountName = SolidFireUtil.getSolidFireAccountName(account.getUuid(), account.getAccountId());
                    SolidFireUtil.SolidFireAccount sfAccount = SolidFireUtil.getSolidFireAccount(sfConnection, sfAccountName);

                    if (sfAccount == null) {
                        sfAccount = createSolidFireAccount(sfConnection, sfAccountName);
                    }

                    SolidFireUtil.updateCsDbWithSolidFireAccountInfo(account.getId(), sfAccount, storagePoolId, _accountDetailsDao);

                    accountDetail = SolidFireUtil.getAccountDetail(volumeInfo.getAccountId(), storagePoolId, _accountDetailsDao);
                }

                long sfAccountId = Long.parseLong(accountDetail.getValue());

                sfVolume = createSolidFireVolume(sfConnection, volumeInfo, sfAccountId);
            }

            iqn = sfVolume.getIqn();

            VolumeVO volume = _volumeDao.findById(volumeInfo.getId());

            volume.set_iScsiName(iqn);
            volume.setFolder(String.valueOf(sfVolume.getId()));
            volume.setPoolType(StoragePoolType.IscsiLUN);
            volume.setPoolId(storagePoolId);

            _volumeDao.update(volume.getId(), volume);

            updateVolumeDetails(volume.getId(), sfVolume.getTotalSize());

            StoragePoolVO storagePool = _storagePoolDao.findById(dataStore.getId());

            long capacityBytes = storagePool.getCapacityBytes();
            // getUsedBytes(StoragePool) will include the bytes of the newly created volume because
            // updateVolumeDetails(long, long) has already been called for this volume
            long usedBytes = getUsedBytes(storagePool);

            storagePool.setUsedBytes(usedBytes > capacityBytes ? capacityBytes : usedBytes);

            _storagePoolDao.update(storagePoolId, storagePool);
        } else if (dataObject.getType() == DataObjectType.SNAPSHOT) {
            SnapshotInfo snapshotInfo = (SnapshotInfo)dataObject;

            long csSnapshotId = snapshotInfo.getId();

            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

            if (snapshotDetails == null || snapshotDetails.getValue() == null) {
                throw new CloudRuntimeException("'createAsync' should not be invoked unless " + SolidFireUtil.SNAPSHOT_ID + " exists.");
            }

            long storagePoolId = dataStore.getId();

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, "tempVolume");

            if (snapshotDetails != null && snapshotDetails.getValue() != null && snapshotDetails.getValue().equalsIgnoreCase("create")) {
                SolidFireUtil.SolidFireVolume sfVolume = createCloneFromSnapshot(sfConnection, csSnapshotId);

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
                throw new CloudRuntimeException("Invalid state in 'createAsync'");
            }
        } else {
            errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to createAsync";
        }

        if (callback != null) {
            // path = iqn
            // size is pulled from DataObject instance, if errMsg is null
            CreateCmdResult result = new CreateCmdResult(iqn, new Answer(null, errMsg == null, errMsg));

            result.setResult(errMsg);

            callback.complete(result);
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

    private long getCsSnapshotIdForCloning(long volumeId) {
        VolumeDetailVO volumeDetail = _volumeDetailsDao.findDetail(volumeId, "cloneOf");

        if (volumeDetail != null && volumeDetail.getValue() != null) {
            return new Long(volumeDetail.getValue());
        }

        return Long.MIN_VALUE;
    }

    private boolean shouldTakeSnapshot(long snapshotId) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotId, "takeSnapshot");

        if (snapshotDetails != null && snapshotDetails.getValue() != null) {
            return new Boolean(snapshotDetails.getValue());
        }

        return false;
    }

    private SolidFireUtil.SolidFireVolume createClone(SolidFireUtil.SolidFireConnection sfConnection, long csSnapshotId, VolumeInfo volumeInfo) {
        String sfNewVolumeName = volumeInfo.getName();

        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

        if (snapshotDetails != null && snapshotDetails.getValue() != null) {
            long sfSnapshotId = Long.parseLong(snapshotDetails.getValue());

            snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

            long sfVolumeId = Long.parseLong(snapshotDetails.getValue());

            long newSfVolumeId = SolidFireUtil.createSolidFireClone(sfConnection, sfVolumeId, sfSnapshotId, sfNewVolumeName, getVolumeAttributes(volumeInfo));

            return SolidFireUtil.getSolidFireVolume(sfConnection, newSfVolumeId);
        }
        else {
            snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

            long sfVolumeId = Long.parseLong(snapshotDetails.getValue());

            long newSfVolumeId = SolidFireUtil.createSolidFireClone(sfConnection, sfVolumeId, sfNewVolumeName, getVolumeAttributes(volumeInfo));

            return SolidFireUtil.getSolidFireVolume(sfConnection, newSfVolumeId);
        }
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

    private SolidFireUtil.SolidFireVolume createCloneFromSnapshot(SolidFireUtil.SolidFireConnection sfConnection, long csSnapshotId) {
        SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.VOLUME_ID);

        long sfVolumeId = Long.parseLong(snapshotDetails.getValue());

        snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

        long sfSnapshotId = Long.parseLong(snapshotDetails.getValue());

        SolidFireUtil.SolidFireSnapshot sfSnapshot = SolidFireUtil.getSolidFireSnapshot(sfConnection, sfVolumeId, sfSnapshotId);

        long newVolumeId = SolidFireUtil.createSolidFireClone(sfConnection, sfVolumeId, sfSnapshotId, sfSnapshot.getName(), null);

        return SolidFireUtil.getSolidFireVolume(sfConnection, newVolumeId);
    }

    private void updateVolumeDetails(long volumeId, long sfVolumeSize) {
        VolumeDetailVO volumeDetailVo = _volumeDetailsDao.findDetail(volumeId, SolidFireUtil.VOLUME_SIZE);

        if (volumeDetailVo == null || volumeDetailVo.getValue() == null) {
            volumeDetailVo = new VolumeDetailVO(volumeId, SolidFireUtil.VOLUME_SIZE, String.valueOf(sfVolumeSize), false);

            _volumeDetailsDao.persist(volumeDetailVo);
        }
    }

    @Override
    public void deleteAsync(DataStore dataStore, DataObject dataObject, AsyncCompletionCallback<CommandResult> callback) {
        String errMsg = null;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            try {
                VolumeInfo volumeInfo = (VolumeInfo)dataObject;
                long volumeId = volumeInfo.getId();

                long storagePoolId = dataStore.getId();

                SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

                deleteSolidFireVolume(sfConnection, volumeInfo);

                _volumeDetailsDao.removeDetails(volumeId);

                StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);

                long usedBytes = getUsedBytes(storagePool, volumeId);

                storagePool.setUsedBytes(usedBytes < 0 ? 0 : usedBytes);

                _storagePoolDao.update(storagePoolId, storagePool);
            }
            catch (Exception ex) {
                s_logger.debug(SolidFireUtil.LOG_PREFIX + "Failed to delete SolidFire volume", ex);

                errMsg = ex.getMessage();
            }
        } else if (dataObject.getType() == DataObjectType.SNAPSHOT) {
            // should return null when no error message
            errMsg = deleteSnapshot((SnapshotInfo)dataObject, dataStore.getId());
        } else {
            errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to deleteAsync";
        }

        CommandResult result = new CommandResult();

        result.setResult(errMsg);

        callback.complete(result);
    }

    @Override
    public void copyAsync(DataObject srcData, DataObject destData, AsyncCompletionCallback<CopyCommandResult> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canCopy(DataObject srcData, DataObject destData) {
        return false;
    }

    @Override
    public void takeSnapshot(SnapshotInfo snapshotInfo, AsyncCompletionCallback<CreateCmdResult> callback) {
        CreateCmdResult result = null;

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

            // For creating a volume, we need to check to make sure a sufficient amount of space remains in the primary storage.
            // For the purpose of "charging" these bytes against storage_pool.capacityBytes, we take the full size of the SolidFire volume.
            if (usedBytes > capacityBytes) {
                throw new CloudRuntimeException("Insufficient amount of space remains in this primary storage to take a snapshot");
            }

            storagePool.setUsedBytes(usedBytes);

            SnapshotObjectTO snapshotObjectTo = (SnapshotObjectTO)snapshotInfo.getTO();

            if (shouldTakeSnapshot(snapshotInfo.getId())) {
                String sfNewSnapshotName = volumeInfo.getName() + "-" + snapshotInfo.getUuid();

                long sfNewSnapshotId = SolidFireUtil.createSolidFireSnapshot(sfConnection, sfVolumeId, sfNewSnapshotName, getSnapshotAttributes(snapshotInfo));

                updateSnapshotDetails(snapshotInfo.getId(), sfVolumeId, sfNewSnapshotId, storagePoolId, sfVolumeSize);

                snapshotObjectTo.setPath("SfSnapshotId=" + sfNewSnapshotId);
            }
            else {
                String sfNewVolumeName = volumeInfo.getName() + "-" + snapshotInfo.getUuid();

                long sfNewVolumeId = SolidFireUtil.createSolidFireVolume(sfConnection, sfNewVolumeName, sfVolume.getAccountId(), sfVolumeSize,
                        sfVolume.isEnable512e(), getSnapshotAttributes(snapshotInfo), sfVolume.getMinIops(), sfVolume.getMaxIops(), sfVolume.getBurstIops());

                SolidFireUtil.SolidFireVolume sfNewVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfNewVolumeId);

                updateSnapshotDetails(snapshotInfo.getId(), sfNewVolumeId, storagePoolId, sfVolumeSize, sfNewVolume.getIqn());

                snapshotObjectTo.setPath("SfVolumeId=" + sfNewVolumeId);
            }

            // Now that we have successfully created a volume or a snapshot, update the space usage in the storage_pool table
            // (even though storage_pool.used_bytes is likely no longer in use).
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

    private void updateSnapshotDetails(long csSnapshotId, long sfVolumeId, long sfNewSnapshotId, long storagePoolId, long sfNewVolumeSize) {
        SnapshotDetailsVO snapshotDetail = new SnapshotDetailsVO(csSnapshotId,
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

    // return null for no error message
    private String deleteSnapshot(SnapshotInfo snapshotInfo, long storagePoolId) {
        String errMsg = null;

        long csSnapshotId = snapshotInfo.getId();

        try {
            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);

            SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(csSnapshotId, SolidFireUtil.SNAPSHOT_ID);

            if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                long sfSnapshotId = Long.parseLong(snapshotDetails.getValue());

                deleteSolidFireSnapshot(sfConnection, csSnapshotId, sfSnapshotId);
            }
            else {
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
            s_logger.debug(SolidFireUtil.LOG_PREFIX + "Failed to delete SolidFire volume. CloudStack snapshot ID: " + csSnapshotId, ex);

            errMsg = ex.getMessage();
        }

        return errMsg;
    }

    @Override
    public void revertSnapshot(SnapshotInfo snapshot, SnapshotInfo snapshotOnPrimaryStore, AsyncCompletionCallback<CommandResult> callback) {
        throw new UnsupportedOperationException("Reverting not supported. Create a template or volume based on the snapshot instead.");
    }

    @Override
    public void resize(DataObject dataObject, AsyncCompletionCallback<CreateCmdResult> callback) {
        String iqn = null;
        String errMsg = null;

        if (dataObject.getType() == DataObjectType.VOLUME) {
            VolumeInfo volumeInfo = (VolumeInfo)dataObject;
            iqn = volumeInfo.get_iScsiName();
            long storagePoolId = volumeInfo.getPoolId();
            long sfVolumeId = Long.parseLong(volumeInfo.getFolder());
            ResizeVolumePayload payload = (ResizeVolumePayload)volumeInfo.getpayload();

            SolidFireUtil.SolidFireConnection sfConnection = SolidFireUtil.getSolidFireConnection(storagePoolId, _storagePoolDetailsDao);
            SolidFireUtil.SolidFireVolume sfVolume = SolidFireUtil.getSolidFireVolume(sfConnection, sfVolumeId);

            verifySufficientIopsForStoragePool(storagePoolId, volumeInfo.getId(), payload.newMinIops);
            verifySufficientStorageForStoragePool(storagePoolId, volumeInfo.getId(), payload.newSize, payload.newHypervisorSnapshotReserve);

            long sfNewVolumeSize = sfVolume.getTotalSize();

            if (payload.newSize != null || payload.newHypervisorSnapshotReserve != null) {
               Integer hsr = s_lowestHypervisorSnapshotReserve;

               if (volumeInfo.getHypervisorSnapshotReserve() != null && hsr < volumeInfo.getHypervisorSnapshotReserve()) {
                   hsr = volumeInfo.getHypervisorSnapshotReserve();
               }

               if (payload.newHypervisorSnapshotReserve != null && payload.newHypervisorSnapshotReserve > hsr) {
                   hsr = payload.newHypervisorSnapshotReserve;
               }

               sfNewVolumeSize = getVolumeSizeIncludingHypervisorSnapshotReserve(payload.newSize, hsr);
            }

            Map<String, String> mapAttributes = new HashMap<>();

            mapAttributes.put(SolidFireUtil.CloudStackVolumeId, String.valueOf(volumeInfo.getId()));
            mapAttributes.put(SolidFireUtil.CloudStackVolumeSize, NumberFormat.getInstance().format(payload.newSize));

            SolidFireUtil.modifySolidFireVolume(sfConnection, sfVolumeId, sfNewVolumeSize, mapAttributes,
                    payload.newMinIops, payload.newMaxIops, getDefaultBurstIops(storagePoolId, payload.newMaxIops));

            VolumeVO volume = _volumeDao.findById(volumeInfo.getId());

            volume.setMinIops(payload.newMinIops);
            volume.setMaxIops(payload.newMaxIops);

            _volumeDao.update(volume.getId(), volume);

            // SolidFireUtil.VOLUME_SIZE was introduced in 4.5.
            // To be backward compatible with releases prior to 4.5, call updateVolumeDetails here.
            // That way if SolidFireUtil.VOLUME_SIZE wasn't put in the volume_details table when the
            // volume was initially created, it can be placed in volume_details if the volume is resized.
            updateVolumeDetails(volume.getId(), sfNewVolumeSize);
        } else {
            errMsg = "Invalid DataObjectType (" + dataObject.getType() + ") passed to resize";
        }

        CreateCmdResult result = new CreateCmdResult(iqn, new Answer(null, errMsg == null, errMsg));

        result.setResult(errMsg);

        callback.complete(result);
    }

    private void verifySufficientIopsForStoragePool(long storagePoolId, long volumeId, long newMinIops) {
        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);
        VolumeVO volume = _volumeDao.findById(volumeId);

        long currentMinIops = volume.getMinIops();
        long diffInMinIops = newMinIops - currentMinIops;

        // if the desire is for more IOPS
        if (diffInMinIops > 0) {
            long usedIops = getUsedIops(storagePool);
            long capacityIops = storagePool.getCapacityIops();

            if (usedIops + diffInMinIops > capacityIops) {
                throw new CloudRuntimeException("Insufficient number of IOPS available in this storage pool");
            }
        }
    }

    private void verifySufficientStorageForStoragePool(long storagePoolId, long volumeId, long newSize, Integer newHypervisorSnapshotReserve) {

        StoragePoolVO storagePool = _storagePoolDao.findById(storagePoolId);
        VolumeVO volume = _volumeDao.findById(volumeId);

        long currentSizeWithHsr = getVolumeSizeIncludingHypervisorSnapshotReserve(volume);

        newHypervisorSnapshotReserve = newHypervisorSnapshotReserve == null ?
                s_lowestHypervisorSnapshotReserve :
                Math.max(newHypervisorSnapshotReserve, s_lowestHypervisorSnapshotReserve);

        long newSizeWithHsr = (long) (newSize + newSize * (newHypervisorSnapshotReserve / 100f));

        if (newSizeWithHsr < currentSizeWithHsr) {
            throw new CloudRuntimeException("Storage " + storagePoolId + " does not support shrinking of volume " + volumeId);
        }

        //XXX: I'm not sure if we want to check this because SolidFire technically stores de-duped data
        long availableBytes = storagePool.getCapacityBytes() - storagePool.getUsedBytes();

        if ( (newSizeWithHsr - currentSizeWithHsr) > availableBytes ) {
            throw new CloudRuntimeException("Storage " + storagePoolId + " does not have enough space for volume " + volumeId);
        }
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

    private void deleteSolidFireSnapshot(SolidFireUtil.SolidFireConnection sfConnection, long csSnapshotId, long sfSnapshotId) {
        SolidFireUtil.deleteSolidFireSnapshot(sfConnection, sfSnapshotId);

        SnapshotVO snapshot = _snapshotDao.findById(csSnapshotId);
        VolumeVO volume = _volumeDao.findById(snapshot.getVolumeId());

        if (volume == null) { // if the volume has been deleted
            List<SnapshotVO> lstSnapshots = getNonDestroyedSnapshots(snapshot.getVolumeId());

            List<SnapshotVO> lstSnapshots2 = new ArrayList<>();

            for (SnapshotVO snapshotVo : lstSnapshots) {
                if (snapshotVo.getId() != csSnapshotId) {
                    SnapshotDetailsVO snapshotDetails = _snapshotDetailsDao.findDetail(snapshotVo.getId(), SolidFireUtil.SNAPSHOT_ID);

                    if (snapshotDetails != null && snapshotDetails.getValue() != null) {
                        lstSnapshots2.add(snapshotVo);
                    }
                }
            }

            if (lstSnapshots2.isEmpty()) {
                volume = _volumeDao.findByIdIncludingRemoved(snapshot.getVolumeId());

                SolidFireUtil.deleteSolidFireVolume(sfConnection, Long.parseLong(volume.getFolder()));
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
}
