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
package org.apache.cloudstack.storage.allocator;

import java.util.ArrayList;
import java.util.List;

import com.cloud.api.query.dao.StorageTagDao;
import com.cloud.api.query.vo.StorageTagVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.dao.UserVmDao;
import org.apache.cloudstack.engine.subsystem.api.storage.StoragePoolAllocator;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.cloud.deploy.DeploymentPlan;
import com.cloud.deploy.DeploymentPlanner.ExcludeList;
import com.cloud.storage.StoragePool;
import com.cloud.user.AccountVO;
import com.cloud.vm.DiskProfile;
import com.cloud.vm.VirtualMachineProfile;

import javax.inject.Inject;

@Component
public class AccountClusterScopeStoragePoolAllocator extends ClusterScopeStoragePoolAllocator {
    private static final Logger LOGGER = Logger.getLogger(AccountClusterScopeStoragePoolAllocator.class);

    @Inject
    private AccountDao accountDao;
    @Inject
    private StorageTagDao storageTagDao;
    @Inject
    private UserVmDao userVmDao;

    @Override
    protected List<StoragePool> select(DiskProfile dskCh, VirtualMachineProfile vmProfile, DeploymentPlan plan, ExcludeList avoid, int returnUpTo) {
        LOGGER.debug("'AccountClusterScopeManagedStoragePoolAllocator' to find storage pool");

        List<StoragePool> storagePools = super.select(dskCh, vmProfile, plan, avoid, StoragePoolAllocator.RETURN_UPTO_ALL);

        if (storagePools == null || storagePools.size() == 0) {
            return new ArrayList<>();
        }

        List<StoragePool> storagePoolsToReturn = new ArrayList<>();

        for (StoragePool storagePool : storagePools) {
            if (isStoragePoolTaggingAcceptable(storagePool, vmProfile.getId())) {
                storagePoolsToReturn.add(storagePool);
            }
            else {
                avoid.addPool(storagePool.getId());
            }
        }

        return returnUpTo(storagePoolsToReturn, returnUpTo);
    }

    private List<String> getStoragePoolTags(long storagePoolId) {
        List<String> storageTagsToReturn = new ArrayList<>();

        List<StorageTagVO> storageTags = storageTagDao.listAll();

        if (storageTags == null) {
            return storageTagsToReturn;
        }

        for (StorageTagVO storageTag : storageTags) {
            if (storageTag.getPoolId() == storagePoolId) {
                storageTagsToReturn.add(storageTag.getName());
            }
        }

        return storageTagsToReturn;
    }

    private boolean isStoragePoolTaggingAcceptable(StoragePool storagePool, long vmId) {
        List<String> storageTags = getStoragePoolTags(storagePool.getId());

        // "SolidFireToAccount" is a special storage tag that lets us know to perform particular checks.
        if (storageTags == null || !storageTags.contains("SolidFireToAccount")) {
            return true;
        }

        UserVmVO userVmVO = userVmDao.findById(vmId);

        if (userVmVO == null) {
            return false;
        }

        long accountId = userVmVO.getAccountId();

        AccountVO accountVO = accountDao.findById(accountId);

        if (accountVO == null) {
            return false;
        }

        String accountName = accountVO.getAccountName();

        if (accountName == null) {
            return false;
        }

        for (String storageTag : storageTags) {
            if (accountName.equalsIgnoreCase(storageTag)) {
                return true;
            }
        }

        return false;
    }

    private List<StoragePool> returnUpTo(List<StoragePool> storagePools, int returnUpTo) {
        List<StoragePool> storagePoolsToReturn = new ArrayList<>();

        for (StoragePool storagePool : storagePools) {
            storagePoolsToReturn.add(storagePool);

            if (storagePoolsToReturn.size() == returnUpTo) {
                return storagePoolsToReturn;
            }
        }

        return storagePoolsToReturn;
    }
}
