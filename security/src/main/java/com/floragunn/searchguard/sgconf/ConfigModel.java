/*
 * Copyright 2015-2018 floragunn GmbH
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

package com.floragunn.searchguard.sgconf;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.transport.TransportAddress;

import com.floragunn.searchguard.authc.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.user.User;

import inet.ipaddr.IPAddress;

public abstract class ConfigModel {
  
    public static final String USER_TENANT = "__user__";

    public abstract Set<String> mapSgRoles(User user, TransportAddress caller);

    public abstract SgRoles getSgRoles();
    
    public abstract Set<String> getAllConfiguredTenantNames();
    
    public abstract boolean isTenantValid(String requestedTenant);
    
    public abstract ActionGroups getActionGroups();

    public abstract List<ClientBlockRegistry<InetAddress>> getBlockIpAddresses();

    public abstract List<ClientBlockRegistry<String>> getBlockedUsers();

    public abstract List<ClientBlockRegistry<IPAddress>> getBlockedNetmasks();

    public interface ActionGroupResolver {
        Set<String> resolvedActions(final List<String> actions);
    }

    
    
   
}
