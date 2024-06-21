/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.test.helper.cluster;

import java.io.FileNotFoundException;
import java.util.Optional;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.test.NodeSettingsSupplier;

public class MinimumSearchGuardSettingsSupplierFactory {

    private final String resourceFolder;
    private final boolean useJksCerts;

    public MinimumSearchGuardSettingsSupplierFactory(String resourceFolder, boolean useJksCerts) {
        this.resourceFolder = resourceFolder;
        this.useJksCerts = useJksCerts;
    }

    public NodeSettingsSupplier minimumSearchGuardSettings(Settings other) {
        return i -> minimumSearchGuardSettingsBuilder(i, false).put(other).build();
    }

    public NodeSettingsSupplier minimumSearchGuardSettingsSslOnly(Settings other) {
        return i -> minimumSearchGuardSettingsBuilder(i, true).put(other).build();
    }

    private Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {

        Settings.Builder builder = Settings.builder();

        builder.put("searchguard.ssl.transport.enforce_hostname_verification", false);

        if (!sslOnly) {
            builder.put("searchguard.background_init_if_sgindex_not_exist", false);
            builder.put("searchguard.ssl_only", false);
        } else {
            builder.put("searchguard.ssl_only", true);
        }

        if (useJksCerts) {
            try {
                final String prefix = Optional.ofNullable(resourceFolder).map(folder -> folder + "/").orElse("");

                builder.put("searchguard.ssl.transport.keystore_alias", "node-0")
                        .put("searchguard.ssl.transport.keystore_filepath",
                                FileHelper.getAbsoluteFilePathFromClassPath(prefix + "node-0-keystore.jks"))
                        .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath(prefix + "truststore.jks"))
                        .put("searchguard.ssl.transport.enforce_hostname_verification", false);

                if (!sslOnly) {
                    builder.putList("searchguard.authcz.admin_dn", "CN=kirk,OU=client,O=client,l=tEst, C=De");
                }

                return builder;
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        return builder;

    }
}
