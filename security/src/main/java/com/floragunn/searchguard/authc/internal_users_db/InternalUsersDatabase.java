/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.internal_users_db;

import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import com.floragunn.searchguard.configuration.ConfigMap;
import com.floragunn.searchguard.configuration.ConfigurationChangeListener;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.modules.state.ComponentState;
import com.floragunn.searchguard.modules.state.ComponentState.State;
import com.floragunn.searchguard.modules.state.ComponentStateProvider;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;

public class InternalUsersDatabase implements ComponentStateProvider {

    private final ComponentState componentState = new ComponentState(100, "internal_users_database", "internal_users_database");
    private volatile ImmutableMap<String, InternalUser> userMap;

    public InternalUsersDatabase(ConfigurationRepository configurationRepository) {
        configurationRepository.subscribeOnChange(new ConfigurationChangeListener() {

            @Override
            public void onChange(ConfigMap configMap) {
                SgDynamicConfiguration<InternalUser> config = configMap.get(CType.INTERNALUSERS);

                if (config != null) {
                    userMap = ImmutableMap.of(config.getCEntries());
                    componentState.setState(State.INITIALIZED);
                    componentState.setConfigVersion(config.getDocVersion());
                    componentState.setMessage(userMap.size() + " users");
                } else {
                    componentState.setState(State.SUSPENDED, "no_configuration");
                }

            }
        });
    }

    public InternalUser get(String userName) throws AuthenticatorUnavailableException {
        if (userMap == null) {
            throw new AuthenticatorUnavailableException("Internal Users Database unavailable", "Internal Users Database is not yet initialized");
        }
        
        return userMap.get(userName);
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

}
