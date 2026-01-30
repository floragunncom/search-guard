/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchguard;

import com.floragunn.searchguard.test.helper.certificate.NodeCertificateType;
import com.floragunn.searchguard.test.helper.certificate.TestCertificates;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import com.floragunn.searchguard.test.helper.log.LogsRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.awaitility.Awaitility.await;

public class SslHostnameVerificationTest {

    @Rule
    public LogsRule logsRule = new LogsRule("com.floragunn.searchguard.ssl.transport.SearchGuardSSLNettyTransport");

    LocalCluster.Builder.Embedded clusterBuilder = new LocalCluster.Builder().embedded().clusterConfiguration(ClusterConfiguration.DEFAULT);

    @Test
    public void shouldStartCluster_invalidSanIpInvalidSanDns_wholeVerificationDisabled() {
        TestCertificates testCertificates = buildTestCertificates("fake", "127.0.0.2");

        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(false, false)).build()) {
            cluster.before();
        } catch (Throwable e) {
            Assert.fail("Cluster should start but instead an exception was thrown : " + e.getMessage());
        }
    }

    @Test
    public void shouldStartCluster_validSanIpValidSanDns_wholeVerificationDisabled() {
        TestCertificates testCertificates = buildTestCertificates("localhost", "127.0.0.1");

        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(false, false)).build()) {
            cluster.before();
        } catch (Throwable e) {
            Assert.fail("Cluster should start but instead an exception was thrown : " + e.getMessage());
        }
    }

    @Test
    public void shouldStartCluster_validSanIpValidSanDns_hostnameResolvingDisabled() {
        TestCertificates testCertificates = buildTestCertificates("localhost", "127.0.0.1");

        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(true, false)).build()) {
            cluster.before();
        } catch (Throwable e) {
            Assert.fail("Cluster should start but instead an exception was thrown : " + e.getMessage());
        }
    }

    @Test
    public void shouldStartCluster_validSanIpInvalidSanDns_hostnameResolvingDisabled() {
        TestCertificates testCertificates = buildTestCertificates("fake", "127.0.0.1");

        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(true, false)).build()) {
            cluster.before();
        } catch (Throwable e) {
            Assert.fail("Cluster should start but instead an exception was thrown : " + e.getMessage());
        }
    }

    @Test
    public void shouldNotStartCluster_invalidSanIpValidSanDns_hostnameResolvingDisabled() {
        TestCertificates testCertificates = buildTestCertificates("localhost", "127.0.0.2");

        ExecutorService executorService = newSingleThreadExecutor();
        Future<Void> clusterFuture = null;
        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(true, false)).build()) {
            clusterFuture = executorService.submit(startCluster(cluster));
            await("expect hostname verification error")
                    .pollDelay(10, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> logsRule.assertThatContain("No subject alternative names matching IP address 127.0.0.1 found"));
        } finally {
            Assert.assertNotNull("clusterFeature is not null", clusterFuture);
            clusterFuture.cancel(true);
            executorService.shutdown();
        }
    }

    @Test
    public void shouldStartCluster_validSanIpValidSanDns_hostnameResolvingEnabled() {
        TestCertificates testCertificates = buildTestCertificates("localhost", "127.0.0.1");

        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(true, true)).build()) {
            cluster.before();
        } catch (Throwable e) {
            Assert.fail("Cluster should start but instead an exception was thrown : " + e.getMessage());
        }
    }

    @Test
    public void shouldNotStartCluster_validSanIpInvalidSanDns_hostnameResolvingEnabled() {
        TestCertificates testCertificates = buildTestCertificates("fake", "127.0.0.1");

        ExecutorService executorService = newSingleThreadExecutor();
        Future<Void> clusterFuture = null;
        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(true, true)).build()) {
            clusterFuture = executorService.submit(startCluster(cluster));
            await("expect hostname verification error")
                    .pollDelay(10, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> logsRule.assertThatContain("No subject alternative names matching IP address 127.0.0.1 found"));
        } finally {
            Assert.assertNotNull("clusterFeature is not null", clusterFuture);
            clusterFuture.cancel(true);
            executorService.shutdown();
        }
    }

    @Test
    public void shouldNotStartCluster_invalidSanIpValidSanDns_hostnameResolvingEnabled() {
        TestCertificates testCertificates = buildTestCertificates("localhost", "127.0.0.2");

        ExecutorService executorService = newSingleThreadExecutor();
        Future<Void> clusterFuture = null;
        try (LocalCluster cluster = clusterBuilder.sslEnabled(testCertificates).nodeSettings(nodeSettings(true, true)).build()) {
            clusterFuture = executorService.submit(startCluster(cluster));
            await("expect hostname verification error")
                    .pollDelay(10, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> logsRule.assertThatContain("No subject alternative names matching IP address 127.0.0.1 found"));
        } finally {
            Assert.assertNotNull("clusterFeature is not null", clusterFuture);
            clusterFuture.cancel(true);
            executorService.shutdown();
        }
    }

    private TestCertificates buildTestCertificates(String dns, String ip) {
        TestCertificates.TestCertificatesBuilder testCertificatesBuilder = TestCertificates.builder()
                .ca()
                .addAdminClients("CN=admin.example.com,OU=Organizational Unit,O=Organization");

        List<String> dnsList = Collections.singletonList(dns);
        List<String> ipList = Collections.singletonList(ip);

        for (int node = 0; node < ClusterConfiguration.DEFAULT.getNodes(); node++) {
            List<String> dn = Collections.singletonList(String.format("CN=node-%d.example.com,OU=Organizational Unit,O=Organization", node));
            testCertificatesBuilder.addNodes(
                    dn, 10, "1.2.3.4.5.5", dnsList, ipList,
                    NodeCertificateType.transport_and_rest, null
            );
        }

        return testCertificatesBuilder.build();
    }

    private Object[] nodeSettings(boolean hostnameVerification, boolean resolveHostname) {
        return new Object[] {
                "searchguard.ssl.transport.enforce_hostname_verification", hostnameVerification,
                "searchguard.ssl.transport.resolve_hostname", resolveHostname
        };
    }

    private Callable<Void> startCluster(LocalCluster cluster) {
        return () -> {
            try {
                cluster.before();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            return null;
        };
    }
}
