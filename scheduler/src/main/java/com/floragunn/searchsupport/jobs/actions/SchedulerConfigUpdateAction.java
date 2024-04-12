/*
 * Copyright 2015-2019 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchsupport.jobs.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;

public class SchedulerConfigUpdateAction extends ActionType<SchedulerConfigUpdateResponse> {
    private final static Logger log = LogManager.getLogger(SchedulerConfigUpdateAction.class);

    public static final SchedulerConfigUpdateAction INSTANCE = new SchedulerConfigUpdateAction();
    public static final String NAME = "cluster:admin/searchsupport/scheduler/config/update";

    protected SchedulerConfigUpdateAction() {
        super(NAME);
    }

    public static void send(Client client, String schedulerName) {
        client.execute(SchedulerConfigUpdateAction.INSTANCE, new SchedulerConfigUpdateRequest(schedulerName),
                new ActionListener<SchedulerConfigUpdateResponse>() {

                    @Override
                    public void onResponse(SchedulerConfigUpdateResponse response) {
                        log.info("Result of scheduler config update of " + schedulerName + ":\n" + response);

                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Scheduler config update of " + schedulerName + " failed", e);
                    }
                });
    }

}
