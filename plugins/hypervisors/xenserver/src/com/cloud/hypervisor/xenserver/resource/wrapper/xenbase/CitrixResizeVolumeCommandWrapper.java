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

package com.cloud.hypervisor.xenserver.resource.wrapper.xenbase;

import com.cloud.utils.exception.CloudRuntimeException;
import com.xensource.xenapi.PBD;
import com.xensource.xenapi.SR;
import org.apache.log4j.Logger;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.storage.ResizeVolumeAnswer;
import com.cloud.agent.api.storage.ResizeVolumeCommand;
import com.cloud.hypervisor.xenserver.resource.CitrixResourceBase;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.xensource.xenapi.Connection;
import com.xensource.xenapi.VDI;

import java.util.HashSet;
import java.util.Set;

@ResourceWrapper(handles =  ResizeVolumeCommand.class)
public final class CitrixResizeVolumeCommandWrapper extends CommandWrapper<ResizeVolumeCommand, Answer, CitrixResourceBase> {

    private static final Logger s_logger = Logger.getLogger(CitrixResizeVolumeCommandWrapper.class);

    @Override
    public Answer execute(final ResizeVolumeCommand command, final CitrixResourceBase citrixResourceBase) {
        final Connection conn = citrixResourceBase.getConnection();
        final String volid = command.getPath();
        final long newSize = command.getNewSize();

        try {

            if (command.isManaged()) {
                //if this is a managed storage, resize the SR too
                //the LUN has already been resized so the SR
                //needs to fill up the new space
                String iScsiName = command.getIscsiName();

                try {

                    final Set<SR> srs = SR.getByNameLabel(conn, iScsiName);
                    Set<PBD> allPbds = new HashSet<>();

                    for (final SR sr : srs) {
                        if (!CitrixResourceBase.SRType.LVMOISCSI.equals(sr.getType(conn))) {
                            continue;
                        }

                        Set<PBD> pbds = sr.getPBDs(conn);

                        if (pbds.size() <= 0) {
                            s_logger.debug("No PBDs found for this SR : " + sr.getNameLabel(conn));
                        }

                        allPbds.addAll(pbds);
                    }

                    for (final PBD pbd: allPbds) {
                        PBD.Record pbdr = pbd.getRecord(conn);
                        if (pbdr.currentlyAttached) {
                            pbd.unplug(conn);
                            pbd.plug(conn);
                        }
                    }
                } catch (Exception e) {
                    throw new CloudRuntimeException("Unable to resize volume :" +  e.getMessage());
                }
            }

            final VDI vdi = citrixResourceBase.getVDIbyUuid(conn, volid);
            vdi.resize(conn, newSize);
            return new ResizeVolumeAnswer(command, true, "success", newSize);
        } catch (final Exception e) {
            s_logger.warn("Unable to resize volume", e);
            final String error = "failed to resize volume:" + e;
            return new ResizeVolumeAnswer(command, false, error);
        }
    }
}