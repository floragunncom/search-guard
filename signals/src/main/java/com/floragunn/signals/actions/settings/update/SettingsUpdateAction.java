/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.actions.settings.update;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.Client;

public class SettingsUpdateAction extends ActionType<SettingsUpdateResponse> {
    private final static Logger log = LogManager.getLogger(SettingsUpdateAction.class);

    public static final SettingsUpdateAction INSTANCE = new SettingsUpdateAction();
    public static final String NAME = "cluster:admin:searchguard:signals:settings/update";

    protected SettingsUpdateAction() {
        super(NAME, in -> {
            SettingsUpdateResponse response = new SettingsUpdateResponse(in);
            return response;
        });
    }

    public static void send(Client client) {
        client.execute(SettingsUpdateAction.INSTANCE, new SettingsUpdateRequest(), new ActionListener<SettingsUpdateResponse>() {

            @Override
            public void onResponse(SettingsUpdateResponse response) {
                log.info("Result of settings update:\n" + response);

            }

            @Override
            public void onFailure(Exception e) {
                log.error("settings update failed", e);
            }
        });
    }

}
