/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.searchguard.authc;

import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.InetAddress;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Authc;
import com.floragunn.searchguard.test.TestSgConfig.Authc.Domain.UserMapping;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

/**
 * Reproduces the {@code NotSerializableException: io.netty.handler.codec.HeadersUtils$1} that occurs when the
 * {@code trusted_origin} auth domain maps user attributes ({@code attrs.from}) from request headers using JSONPath
 * expressions like {@code $.request.headers["x-proxy-attr-..."]}.
 *
 * <p>
 * The attribute value extracted from the Netty-backed request header map keeps a reference to a Netty internal object
 * which is not {@link java.io.Serializable}. As soon as a query has to be dispatched to a shard residing on a different
 * node, {@code SearchGuardInterceptor.ensureCorrectHeaders} tries to Java-serialize the user context for inter-node
 * transport, which fails.
 *
 * <p>
 * The test therefore needs a multi-node cluster so that the cross-shard search forces the user context to be
 * serialized for transport.
 */
public class TrustedOriginHeaderSerializationIntTests {

    private static final String INDEX = "test_index";

    static final TestSgConfig.Role PROXY_USER_ROLE = new TestSgConfig.Role("proxy_user_role")//
            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS_RO", "SGS_CLUSTER_MONITOR")//
            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR").on(INDEX + "*");

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(//
            new Authc.Domain("trusted_origin")//
                    .userMapping(new UserMapping()//
                            .userNameFrom("request.headers.x-proxy-user")//
                            .rolesFromCommaSeparatedString("request.headers.x-proxy-roles")//
                            .attrsFrom("accesslog", "$.request.headers[\"x-proxy-attr-accesslog\"]")//
                            .attrsFrom("aspmanid", "$.request.headers[\"x-proxy-attr-aspmanid\"]")//
                            .attrsFrom("appname", "$.request.headers[\"x-proxy-attr-appname\"]")//
                            .attrsFrom("fqdn", "$.request.headers[\"x-proxy-attr-fqdn\"]")//
                            .attrsFrom("maillog", "$.request.headers[\"x-proxy-attr-maillog\"]")), //
            new Authc.Domain("basic/internal_users_db")//
    ).trustedProxies("127.0.0.10");

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder()//
            .clusterConfiguration(ClusterConfiguration.DEFAULT)//
            .sslEnabled()//
            .authc(AUTHC)//
            .roles(PROXY_USER_ROLE)//
            .roleToRoleMapping(PROXY_USER_ROLE, "admin")//
            .embedded().build();

    @BeforeClass
    public static void initTestData() {
        Client client = cluster.getInternalNodeClient();

        client.admin().indices()
                .create(new CreateIndexRequest(INDEX)
                        .settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build()))
                .actionGet();

        for (int i = 0; i < 20; i++) {
            client.index(new IndexRequest(INDEX).id("doc_" + i).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"value\": " + i + "}", XContentType.JSON)).actionGet();
        }
    }

    @Test
    public void crossShardSearch_withTrustedOriginHeaderAttributes() throws Exception {
        try (GenericRestClient client = cluster.getRestClient()) {
            // Make the request originate from 127.0.0.10, which is configured as a trusted proxy, so it is accepted by
            // the trusted_origin domain (see ClientAddressAscertainer.CIDRBased: a request from a trusted proxy without
            // an X-Forwarded-For header is treated as trusted).
            client.setLocalAddress(InetAddress.getByAddress(new byte[] { 127, 0, 0, 10 }));

            HttpResponse response = client.get("/" + INDEX + "/_search?size=100", //
                    new BasicHeader("x-proxy-user", "user@example.com"), //
                    new BasicHeader("x-proxy-roles", "permit_access_log,admin"), //
                    new BasicHeader("x-proxy-attr-accesslog", "yes"));

            assertThat(response, isOk());
        }
    }
}