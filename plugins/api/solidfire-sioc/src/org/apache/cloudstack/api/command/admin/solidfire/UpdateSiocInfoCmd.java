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
package org.apache.cloudstack.api.command.admin.solidfire;

import javax.inject.Inject;

import com.google.common.base.Optional;

import org.apache.log4j.Logger;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.response.ZoneResponse;
import org.apache.cloudstack.api.response.solidfire.ApiUpdateSiocInfoResponse;
import org.apache.cloudstack.context.CallContext;
import org.apache.cloudstack.solidfire.SolidFireSiocManager;

import com.cloud.user.Account;

@APICommand(name = "updateSiocInfo", responseObject = ApiUpdateSiocInfoResponse.class, description = "Update SIOC info",
        requestHasSensitiveInfo = false, responseHasSensitiveInfo = false)
public class UpdateSiocInfoCmd extends BaseCmd {
    private static final Logger s_logger = Logger.getLogger(UpdateSiocInfoCmd.class.getName());
    private static final String s_name = "updatesiocinforesponse";

    @Parameter(name = ApiConstants.ZONE_ID, type = CommandType.UUID, entityType = ZoneResponse.class, description = "Zone ID", required = true)
    private long _zoneId;

    @Parameter(name = "storagetag", type = CommandType.STRING, description = "Storage Tag to leverage", required = true)
    private String _storageTag;

    @Parameter(name = "sharespergb", type = CommandType.INTEGER, description = "Shares per GB", required = false)
    private Integer _sharesPerGB;

    @Parameter(name = "limitiopspergb", type = CommandType.INTEGER, description = "Limit IOPS per GB", required = false)
    private Integer _limitIopsPerGB;

    @Inject private SolidFireSiocManager _manager;

    /////////////////////////////////////////////////////
    /////////////// API Implementation///////////////////
    /////////////////////////////////////////////////////

    @Override
    public String getCommandName() {
        return s_name;
    }

    @Override
    public long getEntityOwnerId() {
        Account account = CallContext.current().getCallingAccount();

        if (account != null) {
            return account.getId();
        }

        return Account.ACCOUNT_ID_SYSTEM; // no account info given, parent this command to SYSTEM so ERROR events are tracked
    }

    @Override
    public void execute() {
        s_logger.info("'UpdateSiocInfoCmd.execute' method invoked");

        String msg = "Success";

        try {
            _manager.updateSiocInfo(_zoneId, _storageTag, Optional.fromNullable(_sharesPerGB), Optional.fromNullable(_limitIopsPerGB));
        }
        catch (Exception ex) {
            msg = ex.getMessage();
        }

        ApiUpdateSiocInfoResponse response = new ApiUpdateSiocInfoResponse(msg);

        response.setResponseName(getCommandName());
        response.setObjectName("apiupdatesiocinfo");

        setResponseObject(response);
    }
}
