/*
 * Copyright 2015-2017 floragunn GmbH
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

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.http.NoHttpResponseException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.PluginAwareNode;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.floragunn.searchguard.ssl.test.SingleClusterTest;
import com.floragunn.searchguard.ssl.test.helper.file.FileHelper;
import com.floragunn.searchguard.ssl.test.helper.rest.RestHelper;
import com.floragunn.searchguard.ssl.util.ExceptionUtils;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.ssl.util.config.GenericSSLConfig;

import io.netty.util.internal.PlatformDependent;

@SuppressWarnings({"resource"})
public class SSLTest extends SingleClusterTest {

    public static final TimeValue MASTER_NODE_TIMEOUT = new TimeValue(40, TimeUnit.SECONDS);
    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testHttps() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                .put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS, "TLSv1.2","TLSv1.3")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, "TLSv1.2","TLSv1.3")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .build();

        setupSslOnlyMode(settings);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "ssl/node-untspec5-keystore.p12";
        
        //System.out.println(rh.executeSimpleRequest("_searchguard/sslinfo?pretty&show_dn=true"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty&show_dn=true").contains("EMAILADDRESS=unt@tst.com"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty&show_dn=true").contains("local_certificates_list"));
        Assert.assertFalse(rh.executeSimpleRequest("_searchguard/sslinfo?pretty&show_dn=false").contains("local_certificates_list"));
        Assert.assertFalse(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("local_certificates_list"));
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/settings?pretty").contains("\"searchguard\""));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/settings?pretty").contains("keystore_filepath"));
        //Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty&show_server_certs=true").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));

    }
    
    @Test
    public void testCipherAndProtocols() throws Exception {
        
        Security.setProperty("jdk.tls.disabledAlgorithms","");
        //System.out.println("Disabled algos: "+Security.getProperty("jdk.tls.disabledAlgorithms"));

        Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put("searchguard.ssl.http.enabled_ciphers","TLS_RSA_WITH_AES_256_CBC_SHA256")
                .put("searchguard.ssl.http.enabled_protocols","TLSv1.2")
                .put("path.home",".")
                .build();
        
        try {
            String[] enabledCiphers = new DefaultSearchGuardKeyStore(settings, Paths.get(".")).createHTTPSSLEngine().getEnabledCipherSuites();
            String[] enabledProtocols = new DefaultSearchGuardKeyStore(settings, Paths.get(".")).createHTTPSSLEngine().getEnabledProtocols();

            Assert.assertEquals(1, enabledProtocols.length);
            Assert.assertEquals("TLSv1.2", enabledProtocols[0]);
            Assert.assertEquals(1, enabledCiphers.length);
            Assert.assertEquals("TLS_RSA_WITH_AES_256_CBC_SHA256", enabledCiphers[0]);
            
            settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                    .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                    .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                    .put("searchguard.ssl.transport.enabled_ciphers","TLS_RSA_WITH_AES_256_CBC_SHA256")
                    .put("searchguard.ssl.transport.enabled_protocols","TLSv1.2")
                    .put("path.home",".")
                    .build();
            
            enabledCiphers = new DefaultSearchGuardKeyStore(settings, Paths.get(".")).createServerTransportSSLEngine().getEnabledCipherSuites();
            enabledProtocols = new DefaultSearchGuardKeyStore(settings, Paths.get(".")).createServerTransportSSLEngine().getEnabledProtocols();

            Assert.assertEquals(1, enabledProtocols.length);
            Assert.assertEquals("TLSv1.2", enabledProtocols[0]);
            Assert.assertEquals(1, enabledCiphers.length);
            Assert.assertEquals("TLS_RSA_WITH_AES_256_CBC_SHA256", enabledCiphers[0]);

            enabledCiphers = new DefaultSearchGuardKeyStore(settings, Paths.get(".")).createClientTransportSSLEngine(null, -1).getEnabledCipherSuites();
            enabledProtocols = new DefaultSearchGuardKeyStore(settings, Paths.get(".")).createClientTransportSSLEngine(null, -1).getEnabledProtocols();

            Assert.assertEquals(1, enabledProtocols.length);
            Assert.assertEquals("TLSv1.2", enabledProtocols[0]);
            Assert.assertEquals(1, enabledCiphers.length);
            Assert.assertEquals("TLS_RSA_WITH_AES_256_CBC_SHA256", enabledCiphers[0]);
        } catch (ElasticsearchSecurityException e) {
            //System.out.println("EXPECTED "+e.getClass().getSimpleName()+" for "+System.getProperty("java.specification.version")+": "+e.toString());
            //e.printStackTrace();
            Assert.assertTrue("Check if error contains 'no valid cipher suites' -> "+e.toString(),e.toString().contains("no valid cipher suites")
                    || e.toString().contains("failed to set cipher suite")
                    || e.toString().contains("Unable to configure permitted SSL ciphers")
                    || e.toString().contains("OPENSSL_internal:NO_CIPHER_MATCH")
                   );
        }
    }
    
    @Test
    public void testHttpsOptionalAuth() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;

        //System.out.println(rh.executeSimpleRequest("_searchguard/sslinfo?pretty"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/settings?pretty").contains("\"searchguard\""));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
    }
    
    @Test
    public void testHttpsAndNodeSSL() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true).put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        
        //System.out.println(rh.executeSimpleRequest("_searchguard/sslinfo?pretty"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_count\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_count\" : 0"));
    
    }
    
    @Test
    public void testHttpsAndNodeSSLPem() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem"))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, "changeit")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/root-ca.pem"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem"))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_PASSWORD, "changeit")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/root-ca.pem"))
                .build();

        setupSslOnlyMode(settings);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        
        //System.out.println(rh.executeSimpleRequest("_searchguard/sslinfo?pretty"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        //Assert.assertTrue(!executeSimpleRequest("_searchguard/sslinfo?pretty").contains("null"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
    }

    @Test
    public void testHttpsAndNodeSSLFailedCipher() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true).put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
      
                .put("searchguard.ssl.transport.enabled_ciphers","INVALID_CIPHER")
                
                .build();

        try {
            setupSslOnlyMode(settings);
            Assert.fail();
        } catch (Exception e1) {
            //e1.printStackTrace();
            //System.out.println("##1 "+e1.toString());
            Throwable e = ExceptionUtils.getRootCause(e1);
            Assert.assertTrue(e.toString(), e.toString().contains("no valid cipher"));
        }
    }

    @Test
    public void testHttpPlainFail() throws Exception {
        thrown.expect(NoHttpResponseException.class);

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "OPTIONAL")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        setupSslOnlyMode(settings);
        
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = false;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = false;
        
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
    }

    @Test
    public void testHttpsNoEnforce() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "NONE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = false;
        
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));

        Assert.assertFalse(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
    }

    @Test
    public void testHttpsEnforceFail() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        setupSslOnlyMode(settings);

        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = false;
        
        try {
            rh.executeSimpleRequest("");
            Assert.fail();
        } catch (SocketException | SSLException e) {
            //expected
            //System.out.println("Expected SSLHandshakeException "+e.toString());
        } catch (Exception e) {
            //e.printStackTrace();
            Assert.fail("Unexpected exception "+e.toString());
        }
    }

    @Test
    public void testHttpsV3Fail() throws Exception {
        thrown.expect(SSLHandshakeException.class);

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "NONE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks")).build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = false;
        rh.enableHTTPClientSSLv3Only = true;
        
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
    }

    @Test
    public void testNodeClientSSL() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = nonSslRestHelper();

        final Settings tcSettings = Settings.builder().put("cluster.name", clusterInfo.clustername)
                .put("path.home", tmpHome())
                .put("node.name", "client_node_" + new Random().nextInt())
                .put("node.roles", "")
                .put("discovery.initial_state_timeout","8s")
                .putList("cluster.initial_master_nodes", clusterInfo.tcpMasterPortsOnly)
                .putList("discovery.seed_hosts",         clusterInfo.tcpMasterPortsOnly)
                .put(settings)// -----
                .build();
        
        try (Node node = new PluginAwareNode(false, tcSettings).start()) {
            ClusterHealthResponse res = node.client().admin().cluster().health(new ClusterHealthRequest(MASTER_NODE_TIMEOUT).waitForNodes("4").timeout(TimeValue.timeValueSeconds(15))).actionGet();
            Assert.assertFalse(res.isTimedOut());
            Assert.assertEquals(4, res.getNumberOfNodes());
            Assert.assertEquals(4, node.client().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodes().size());
        }

        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_count\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_count\" : 0"));
    }

    private String tmpHome() {
        try {
            File tmp = Files.createTempDirectory("sslclientnode").toFile();
            tmp.deleteOnExit();
            return tmp.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAvailCiphers() throws Exception {
        final SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(null, null, null);
        final SSLEngine engine = serverContext.createSSLEngine();
        final List<String> jdkSupportedCiphers = new ArrayList<>(Arrays.asList(engine.getSupportedCipherSuites()));
        jdkSupportedCiphers.retainAll(SSLConfigConstants.getSecureSSLCiphers(Settings.EMPTY, false));
        engine.setEnabledCipherSuites(jdkSupportedCiphers.toArray(new String[0]));

        final List<String> jdkEnabledCiphers = Arrays.asList(engine.getEnabledCipherSuites());
        // example
        // TLS_RSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
        // TLS_RSA_WITH_AES_128_CBC_SHA, TLS_DHE_RSA_WITH_AES_128_CBC_SHA
        //System.out.println("JDK enabled ciphers: " + jdkEnabledCiphers);
        Assert.assertTrue(jdkEnabledCiphers.size() > 0);
    }
    
    @Test
    public void testUnmodifieableCipherProtocolConfig() throws Exception {
        SSLConfigConstants.getSecureSSLProtocols(Settings.EMPTY, false)[0] = "bogus";
        Assert.assertEquals("TLSv1.3", SSLConfigConstants.getSecureSSLProtocols(Settings.EMPTY, false)[0]);
        
        try {
            SSLConfigConstants.getSecureSSLCiphers(Settings.EMPTY, false).set(0, "bogus");
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            //expected
        }
    }

    @Test
    public void testCRLPem() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem"))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, "changeit")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/root-ca.pem"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0.key.pem"))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_PASSWORD, "changeit")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/chain-ca.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_VALIDATE, true)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_VALIDATION_DATE, CertificateValidatorTest.CRL_DATE.getTime())
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
    }
    
    @Test
    public void testCRL() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", false)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0").put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_VALIDATE, true)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_FILE, FileHelper. getAbsoluteFilePathFromClassPath("ssl/crl/revoked.crl"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_CRL_VALIDATION_DATE, CertificateValidatorTest.CRL_DATE.getTime())
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;

        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        
    }
    
    @Test
    public void testNodeClientSSLwithJavaTLSv13() throws Exception {
        
        //Java TLS 1.3 is available since Java 11
        Assume.assumeTrue(PlatformDependent.javaVersion() >= 11);

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, "TLSv1.3")
                .putList(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, "TLS_AES_128_GCM_SHA256")
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = nonSslRestHelper();

        final Settings tcSettings = Settings.builder().put("cluster.name", clusterInfo.clustername)
                .put("path.home", tmpHome())
                .put("node.name", "client_node_" + new Random().nextInt())
                .put("node.roles", "")
                .put("discovery.initial_state_timeout","18s")
                .putList("cluster.initial_master_nodes", clusterInfo.tcpMasterPortsOnly)
                .putList("discovery.seed_hosts",         clusterInfo.tcpMasterPortsOnly)
                .put(settings)// -----
                .build();

        try (Node node = new PluginAwareNode(false, tcSettings).start()) {
            ClusterHealthResponse res = node.client().admin().cluster().health(new ClusterHealthRequest(MASTER_NODE_TIMEOUT).waitForNodes("4").timeout(TimeValue.timeValueSeconds(25))).actionGet();
            Assert.assertFalse(res.isTimedOut());
            Assert.assertEquals(4, res.getNumberOfNodes());
            Assert.assertEquals(4, node.client().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodes().size());
        }

        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_count\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_count\" : 0"));
    }

    @Test
    public void testHttpsAndNodeSSLKeyPass() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_KEYPASSWORD, "changeit")
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true).put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_KEYPASSWORD, "changeit")

                
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        
        //System.out.println(rh.executeSimpleRequest("_searchguard/sslinfo?pretty"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("CN=node-0.example.com,OU=SSL,O=Test,L=Test,C=DE"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_count\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"rx_size_in_bytes\" : 0"));
        Assert.assertFalse(rh.executeSimpleRequest("_nodes/stats?pretty").contains("\"tx_count\" : 0"));
    
    }
    
    @Test(expected=ElasticsearchSecurityException.class)
    public void testHttpsAndNodeSSLKeyPassFail() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS, "node-0")
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS, "node-0")
                .put("searchguard.ssl.transport.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_KEYPASSWORD, "wrongpass")
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true).put("searchguard.ssl.http.clientauth_mode", "REQUIRE")
                .put("searchguard.ssl.http.keystore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath", FileHelper. getAbsoluteFilePathFromClassPath("ssl/truststore.jks"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_KEYSTORE_KEYPASSWORD, "wrongpass")

                
                .build();

        setupSslOnlyMode(settings);
        
        RestHelper rh = restHelper();
        rh.enableHTTPClientSSL = true;
        rh.trustHTTPServerCertificate = true;
        rh.sendHTTPClientCertificate = true;
        
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
    
    }
    
    @Test
    public void testHttpsAndNodeSSLPCKS1() throws Exception {

        final Settings settings = Settings.builder().put("searchguard.ssl.transport.enabled", true)
                
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/pkcs1/node-0.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/pkcs1/node-0-pkcs1.key.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/root-ca.pem"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.resolve_hostname", false)

                .put("searchguard.ssl.http.enabled", true)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/pkcs1/node-0.crt.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/pkcs1/node-0-pkcs1.key.pem"))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH, FileHelper. getAbsoluteFilePathFromClassPath("ssl/root-ca.pem"))
                .build();

        setupSslOnlyMode(settings);

        RestHelper rh = restHelper();
        
        try {
        
        
        rh.enableHTTPClientSSL = true;
        rh.setSslConfig(new GenericSSLConfig.Builder().trustAll(true).build());
        
        //System.out.println(rh.executeSimpleRequest("_searchguard/sslinfo?pretty"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").contains("TLS"));
        Assert.assertTrue(rh.executeSimpleRequest("_searchguard/sslinfo?pretty").length() > 0);
        Assert.assertTrue(rh.executeSimpleRequest("_nodes/settings?pretty").contains(clusterInfo.clustername));
        } finally {
        	rh.setSslConfig(null);
        }
    }
}
