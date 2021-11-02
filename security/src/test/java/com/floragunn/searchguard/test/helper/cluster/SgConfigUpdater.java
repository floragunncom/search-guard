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

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.sgconf.impl.CType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.function.Supplier;

class SgConfigUpdater {

    private static final Logger log = LogManager.getLogger(SgConfigUpdater.class);

    static void updateSgConfig(Supplier<Client> adminCertClientSupplier, CType configType, String key, Map<String, Object> value) {
        try (Client client = adminCertClientSupplier.get()) {
            log.info("Updating config {}.{}:{}", configType, key, value);

            GetResponse getResponse = client.get(new GetRequest("searchguard", configType.toLCString())).actionGet();
            String jsonDoc = new String(Base64.getDecoder().decode(String.valueOf(getResponse.getSource().get(configType.toLCString()))));
            NestedValueMap config = NestedValueMap.fromJsonString(jsonDoc);

            config.put(key, value);

            if (log.isTraceEnabled()) {
                log.trace("Updated config: " + config);
            }

            IndexResponse response = client
                    .index(new IndexRequest("searchguard").id(configType.toLCString()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .source(configType.toLCString(), BytesReference.fromByteBuffer(ByteBuffer.wrap(config.toJsonString().getBytes("utf-8")))))
                    .actionGet();

            if (response.getResult() != DocWriteResponse.Result.UPDATED) {
                throw new RuntimeException("Updated failed " + response);
            }

            ConfigUpdateResponse configUpdateResponse = client
                    .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]))).actionGet();

            if (configUpdateResponse.hasFailures()) {
                throw new RuntimeException("ConfigUpdateResponse produced failures: " + configUpdateResponse.failures());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
