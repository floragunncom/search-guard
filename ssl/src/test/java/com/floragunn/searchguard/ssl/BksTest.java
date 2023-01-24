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
package com.floragunn.searchguard.ssl;

import com.floragunn.searchguard.ssl.test.SingleClusterTest;
import com.floragunn.searchguard.ssl.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.ssl.test.helper.file.FileHelper;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

public class BksTest extends SingleClusterTest {

    @Test
    public void testBksUnsupported() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_TYPE, "BKS-V1").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.bks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        try {
            setupSslOnlyMode(settings, ClusterConfiguration.SINGLENODE);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.toString(), e.getCause().getCause().getMessage().contains("Keystores of type BKS-V1 are not supported"));
        }
    }

}
