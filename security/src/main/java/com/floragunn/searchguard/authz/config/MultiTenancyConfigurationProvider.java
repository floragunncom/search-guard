/*
 * Copyright 2023-2025 floragunn GmbH
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

package com.floragunn.searchguard.authz.config;

import com.floragunn.searchguard.authz.TenantAccessMapper;
import jakarta.annotation.Nullable;

import java.util.Collections;
import java.util.List;

public interface MultiTenancyConfigurationProvider {

    boolean isMultiTenancyEnabled();

    @Nullable
    String getKibanaServerUser();

    @Nullable
    String getKibanaIndex();

    TenantAccessMapper getTenantAccessMapper();

    boolean isGlobalTenantEnabled();

    boolean isPrivateTenantEnabled();

    List<String> getPreferredTenants();

    MultiTenancyConfigurationProvider DEFAULT = new MultiTenancyConfigurationProvider() {
        @Override
        public boolean isMultiTenancyEnabled() {
            return false;
        }

        @Nullable
        @Override
        public String getKibanaServerUser() {
            return null;
        }

        @Nullable
        @Override
        public String getKibanaIndex() {
            return null;
        }

        @Override
        public TenantAccessMapper getTenantAccessMapper() {
            return TenantAccessMapper.NO_OP;
        }

        @Override
        public boolean isGlobalTenantEnabled() {
            return true;
        }

        @Override
        public boolean isPrivateTenantEnabled() {
            return false;
        }

        @Override
        public List<String> getPreferredTenants() {
            return Collections.emptyList();
        }
    };
}
