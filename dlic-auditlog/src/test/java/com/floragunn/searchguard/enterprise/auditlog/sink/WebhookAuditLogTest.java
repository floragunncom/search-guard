/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.enterprise.auditlog.sink;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.floragunn.searchsupport.util.EsLogging;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.searchguard.enterprise.auditlog.helper.LoggingSink;
import com.floragunn.searchguard.enterprise.auditlog.helper.MockAuditMessageFactory;
import com.floragunn.searchguard.enterprise.auditlog.helper.TestHttpHandler;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.Category;
import com.floragunn.searchguard.enterprise.auditlog.sink.AuditLogSink;
import com.floragunn.searchguard.enterprise.auditlog.sink.WebhookSink;
import com.floragunn.searchguard.enterprise.auditlog.sink.WebhookSink.WebhookFormat;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.network.PortAllocator;
import com.floragunn.searchguard.support.ConfigConstants;
import org.junit.ClassRule;

public class WebhookAuditLogTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    protected HttpServer server = null;

    @Before
    @After
    public void tearDown() {
        if(server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                //ignore
            }
        }
    }
    
	@Test
	public void invalidConfFallbackTest() throws Exception {
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage();

		// provide no settings, fallback must be used
		Settings settings = Settings.builder()
		        .put("path.home", ".")
		        .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
		        .build();
		LoggingSink fallback = new LoggingSink("test", Settings.EMPTY, null, null);
		MockWebhookAuditLog auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, fallback);
		auditlog.store(msg);
		// Webhook sink has failed ...
		Assert.assertEquals(null, auditlog.webhookFormat);
		// ... so message must be stored in fallback
		Assert.assertEquals(1, fallback.messages.size());
		Assert.assertEquals(msg, fallback.messages.get(0));

	}

	@Test
	public void formatsTest() throws Exception {

		String url = "http://localhost";
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage();

		// provide no format, defaults to TEXT
		Settings settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("path.home", ".")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
				.build();
		
		MockWebhookAuditLog auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null);
		auditlog.store(msg);
		Assert.assertEquals(WebhookFormat.TEXT, auditlog.webhookFormat);
		Assert.assertEquals(ContentType.TEXT_PLAIN, auditlog.webhookFormat.getContentType());
		Assert.assertTrue(auditlog.payload, !auditlog.payload.startsWith("{\"text\":"));

		// provide faulty format, defaults to TEXT
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "idonotexist")
				.put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.put("path.home", ".")
				.build();
		auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null);
		auditlog.store(msg);
		Assert.assertEquals(WebhookFormat.TEXT, auditlog.webhookFormat);
		Assert.assertEquals(ContentType.TEXT_PLAIN, auditlog.webhookFormat.getContentType());
		Assert.assertTrue(auditlog.payload, !auditlog.payload.startsWith("{\"text\":"));
		auditlog.close();

		// TEXT
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "text")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))				
				.put("path.home", ".")
				.build();
		auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null);
		auditlog.store(msg);
		Assert.assertEquals(WebhookFormat.TEXT, auditlog.webhookFormat);
		Assert.assertEquals(ContentType.TEXT_PLAIN, auditlog.webhookFormat.getContentType());
		Assert.assertTrue(auditlog.payload, !auditlog.payload.startsWith("{\"text\":"));
		Assert.assertTrue(auditlog.payload, auditlog.payload.contains(AuditMessage.UTC_TIMESTAMP));
		Assert.assertTrue(auditlog.payload, auditlog.payload.contains("audit_request_remote_address"));

		// JSON
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "json")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
                .put("path.home", ".")
				.build();
		auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null);
		auditlog.store(msg);
		//System.out.println(auditlog.payload);
		Assert.assertEquals(WebhookFormat.JSON, auditlog.webhookFormat);
		Assert.assertEquals(ContentType.APPLICATION_JSON, auditlog.webhookFormat.getContentType());
		Assert.assertTrue(auditlog.payload, !auditlog.payload.startsWith("{\"text\":"));
		Assert.assertTrue(auditlog.payload, auditlog.payload.contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertTrue(auditlog.payload, auditlog.payload.contains("audit_request_remote_address"));

		// SLACK
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "slack")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))				
				.put("path.home", ".")
				.build();
		auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null);
		auditlog.store(msg);
		Assert.assertEquals(WebhookFormat.SLACK, auditlog.webhookFormat);
		Assert.assertEquals(ContentType.APPLICATION_JSON, auditlog.webhookFormat.getContentType());
		Assert.assertTrue(auditlog.payload, auditlog.payload.startsWith("{\"text\":"));
		Assert.assertTrue(auditlog.payload, auditlog.payload.contains(AuditMessage.UTC_TIMESTAMP));
        Assert.assertTrue(auditlog.payload, auditlog.payload.contains("audit_request_remote_address"));
	}

    
    
	@Test
	public void invalidUrlTest() throws Exception {

		String url = "faultyurl";

		final Settings settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "slack")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.put("path.home", ".")
				.build();
		LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);;
		MockWebhookAuditLog auditlog = new MockWebhookAuditLog(settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, fallback);
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
		auditlog.store(msg);
		Assert.assertEquals(null, auditlog.url);
		Assert.assertEquals(null, auditlog.payload);
		Assert.assertEquals(null, auditlog.webhookUrl);
		// message must be stored in fallback
		Assert.assertEquals(1, fallback.messages.size());
		Assert.assertEquals(msg, fallback.messages.get(0));
	}

	@Test
	public void noServerRunningHttpTest() throws Exception {
		String url = "http://localhost:8080/endpoint";

		Settings settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "slack")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.put("path.home", ".")
				.build();

		LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);;
		WebhookSink auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
		auditlog.store(msg);
		// can't connect, no server running ...
		Assert.assertEquals("http://localhost:8080/endpoint", auditlog.webhookUrl);
		// ... message must be stored in fallback
		Assert.assertEquals(1, fallback.messages.size());
		Assert.assertEquals(msg, fallback.messages.get(0));		
	}
	

	@Test
	public void postGetHttpTest() throws Exception {
		TestHttpHandler handler = new TestHttpHandler();
        int port = PortAllocator.TCP.allocateSingle(WebhookAuditLogTest.class.getName(), 8090);

		server = ServerBootstrap.bootstrap()
				.setListenerPort(port)
				.setServerInfo("Test/1.1")
				.registerHandler("*", handler)
				.create();

		server.start();

		String url = "http://localhost:" + port + "/endpoint";

		// SLACK
		Settings settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "slack")
				.put("path.home", ".")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.build();

		LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);;
		WebhookSink auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
		auditlog.store(msg);
		Assert.assertTrue(handler.method.equals("POST"));
		Assert.assertTrue(handler.body != null);
		Assert.assertTrue(handler.body.startsWith("{\"text\":"));
		assertStringContainsAllKeysAndValues(handler.body);
		// no message stored on fallback
		Assert.assertEquals(0, fallback.messages.size());
		handler.reset();

		// TEXT
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "texT")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.put("path.home", ".")
				.build();

		auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		auditlog.store(msg);
		Assert.assertTrue(handler.method.equals("POST"));
		Assert.assertTrue(handler.body != null);
		//System.out.println(handler.body);
		Assert.assertFalse(handler.body.contains("{"));
		assertStringContainsAllKeysAndValues(handler.body);
		handler.reset();
		
		// JSON
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "JSon")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.put("path.home", ".")
				.build();

		auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		auditlog.store(msg);
		Assert.assertTrue(handler.method.equals("POST"));
		Assert.assertTrue(handler.body != null);
		Assert.assertTrue(handler.body.contains("{"));
		assertStringContainsAllKeysAndValues(handler.body);
		handler.reset();
		
		// URL POST
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "URL_PARAMETER_POST")
				.put("path.home", ".")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.build();
		
		auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		auditlog.store(msg);
		Assert.assertTrue(handler.method.equals("POST"));
		Assert.assertTrue(handler.body.equals(""));
		Assert.assertTrue(!handler.body.contains("{"));
		assertStringContainsAllKeysAndValues(URLDecoder.decode(handler.uri, StandardCharsets.UTF_8.displayName()));
		handler.reset();
		
		// URL GET
		settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "URL_PARAMETER_GET")
				.put("path.home", ".")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.build();

		auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		auditlog.store(msg);
		Assert.assertTrue(handler.method.equals("GET"));
		Assert.assertEquals(null, handler.body);
		assertStringContainsAllKeysAndValues(URLDecoder.decode(handler.uri, StandardCharsets.UTF_8.displayName()));
		server.shutdown(3l, TimeUnit.SECONDS);
	}

	@Test
	public void httpsTestWithoutTLSServer() throws Exception {

		TestHttpHandler handler = new TestHttpHandler();

		int port = PortAllocator.TCP.allocateSingle(WebhookAuditLogTest.class.getName(), 8080);
		
		server = ServerBootstrap.bootstrap()
				.setListenerPort(port)
				.setServerInfo("Test/1.1")
				.registerHandler("*", handler)
				.create();

		server.start();

		String url = "https://localhost:" + port + "/endpoint";

		Settings settings = Settings.builder()
				.put("searchguard.audit.config.webhook.url", url)
				.put("searchguard.audit.config.webhook.format", "slack")
				.put("path.home", ".")
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))
				.build();

		LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);;
		WebhookSink auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
		AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
		auditlog.store(msg);
		Assert.assertTrue(handler.method == null);
		Assert.assertTrue(handler.body == null);
		Assert.assertTrue(handler.uri == null);
		// ... so message must be stored in fallback
		Assert.assertEquals(1, fallback.messages.size());
		Assert.assertEquals(msg, fallback.messages.get(0));
		server.shutdown(3l, TimeUnit.SECONDS);
	}


	@Test
    public void httpsTest() throws Exception {

        TestHttpHandler handler = new TestHttpHandler();

        int port = PortAllocator.TCP.allocateSingle(WebhookAuditLogTest.class.getName(), 8090);

        
        server = ServerBootstrap.bootstrap()
                .setListenerPort(port)
                .setServerInfo("Test/1.1")
                .setSslContext(createSSLContext())
                .registerHandler("*", handler)
                .create();

        server.start();
        AuditMessage msg = MockAuditMessageFactory.validAuditMessage();

        String url = "https://localhost:" + port + "/endpoint";
        
        // try with ssl verification on, no trust ca, must fail
        Settings settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "slack")
                .put("path.home", ".")
                .put("searchguard.audit.config.webhook.ssl.verify", true)                
                .build();

		LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);
		WebhookSink auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);        
		// message must be stored in fallback
		Assert.assertEquals(1, fallback.messages.size());
		Assert.assertEquals(msg, fallback.messages.get(0));
        
        // disable ssl verification, no ca, call must succeed 
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.audit.config.webhook.ssl.verify", false)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);

        // enable ssl verification, provide correct trust ca, call must succeed
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);

        // enable ssl verification, provide wrong trust ca, call must succeed
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore_fail.jks"))                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);        
                
        server.shutdown(3l, TimeUnit.SECONDS);
    }

	@Test
    public void httpsTestPemDefault() throws Exception {

        TestHttpHandler handler = new TestHttpHandler();

        int port = PortAllocator.TCP.allocateSingle(WebhookAuditLogTest.class.getName(), 8090);

        
        server = ServerBootstrap.bootstrap()
                .setListenerPort(port)
                .setServerInfo("Test/1.1")
                .setSslContext(createSSLContext())
                .registerHandler("*", handler)
                .create();

        server.start();
        AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
        LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);
        
        String url = "https://localhost:" + port + "/endpoint";
        
        // test default with filepath
        handler.reset();
        Settings settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.audit.config.webhook.ssl.pemtrustedcas_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/root-ca.pem"))                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        AuditLogSink auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);

        // test default with missing filepath and fallback to correct SG settings
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")                            
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);

        // test default with wrong filepath and fallback to wrong SG settings
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.audit.config.webhook.ssl.pemtrustedcas_filepath", "wrong")                
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore_fail.jks"))                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body); 

        // test default with wrong/no filepath and no fallback to SG settings, must fail
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.ssl.pemtrustedcas_filepath", "wrong")      
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);  

        // test default with existing but wrong PEM, no fallback
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.audit.config.webhook.ssl.pemtrustedcas_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/spock.crt.pem"))                                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);  

        // test default with existing but wrong PEM, fallback present but pemtrustedcas_filepath takes precedence and must fail
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.config.webhook.url", url)
                .put("searchguard.audit.config.webhook.format", "jSoN")
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))                
                .put("searchguard.audit.config.webhook.ssl.pemtrustedcas_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/spock.crt.pem"))                                
                .put("searchguard.audit.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DEFAULT, null, fallback);
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);    
        server.shutdown(3l, TimeUnit.SECONDS);
	}

	@Test
    public void httpsTestPemEndpoint() throws Exception {

        TestHttpHandler handler = new TestHttpHandler();

        int port = PortAllocator.TCP.allocateSingle(WebhookAuditLogTest.class.getName(), 8090);
        
        server = ServerBootstrap.bootstrap()
                .setListenerPort(port)
                .setServerInfo("Test/1.1")
                .setSslContext(createSSLContext())
                .registerHandler("*", handler)
                .create();

        server.start();
        AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
        LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);
        
        String url = "https://localhost:" + port + "/endpoint";
        
        // test default with filepath
        handler.reset();
        Settings settings = Settings.builder()
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.url", url)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.format", "jSoN")
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.pemtrustedcas_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/root-ca.pem"))                
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        AuditLogSink auditlog = new WebhookSink("name", settings, "searchguard.audit.endpoints.endpoint1.config", null, fallback);
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);

        // test default with missing filepath and fallback to correct SG settings
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.url", url)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.format", "jSoN")
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.verify", true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks"))                
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, "searchguard.audit.endpoints.endpoint1.config", null, fallback);        
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);

        // test default with wrong filepath and fallback to wrong SG settings
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.url", url)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.format", "jSoN")
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.verify", true)
                .put("searchguard.ssl.transport.truststore_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore_fail.jks"))                
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, "searchguard.audit.endpoints.endpoint1.config", null, fallback);        
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body); 

        // test default with wrong/no filepath and no fallback to SG settings, must fail
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.url", url)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.format", "jSoN")
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, "searchguard.audit.endpoints.endpoint1.config", null, fallback);        
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);  

        // test default with existing but wrong PEM, no fallback
        handler.reset();
        settings = Settings.builder()
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.url", url)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.format", "jSoN")
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.verify", true)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.pemtrustedcas_filepath", FileHelper.getAbsoluteFilePathFromClassPath("auditlog/spock.crt.pem"))
                .put("path.home", ".")
                .build();
        auditlog = new WebhookSink("name", settings, "searchguard.audit.endpoints.endpoint1.config", null, fallback);        
        auditlog.store(msg);
        Assert.assertNull(handler.method);
        Assert.assertNull(handler.body);
        Assert.assertNull(handler.body);  

        server.shutdown(3l, TimeUnit.SECONDS);
	}

	@Test
    public void httpsTestPemContentEndpoint() throws Exception {

        TestHttpHandler handler = new TestHttpHandler();

        int port = PortAllocator.TCP.allocateSingle(WebhookAuditLogTest.class.getName(), 8090);
        
        server = ServerBootstrap.bootstrap()
                .setListenerPort(port)
                .setServerInfo("Test/1.1")
                .setSslContext(createSSLContext())
                .registerHandler("*", handler)
                .create();

        server.start();
        AuditMessage msg = MockAuditMessageFactory.validAuditMessage();
        LoggingSink fallback =  new LoggingSink("test", Settings.EMPTY, null, null);
        
        String url = "https://localhost:" + port + "/endpoint";
        
        // test  with filecontent
        handler.reset();
        Settings settings = Settings.builder()
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.url", url)
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.format", "jSoN")
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.pemtrustedcas_content", FileHelper.loadFile("auditlog/root-ca.pem"))                
                .put("searchguard.audit.endpoints.endpoint1.config.webhook.ssl.verify", true)
                .put("path.home", ".")
                .build();
        
        AuditLogSink auditlog = new WebhookSink("name", settings, "searchguard.audit.endpoints.endpoint1.config", null, fallback);
        auditlog.store(msg);
        Assert.assertTrue(handler.method.equals("POST"));
        Assert.assertTrue(handler.body != null);
        Assert.assertTrue(handler.body.contains("{"));
        assertStringContainsAllKeysAndValues(handler.body);



        server.shutdown(3l, TimeUnit.SECONDS);
	}
	
	// for TLS support on our in-memory server
	private SSLContext createSSLContext() throws Exception {
			final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
					.getDefaultAlgorithm());
			final KeyStore trustStore = KeyStore.getInstance("JKS");
			InputStream trustStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/truststore.jks").toFile());
			trustStore.load(trustStream, "changeit".toCharArray());
			tmf.init(trustStore);

			final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());			
			final KeyStore keyStore = KeyStore.getInstance("JKS");
			InputStream keyStream = new FileInputStream(FileHelper.getAbsoluteFilePathFromClassPath("auditlog/node-0-keystore.jks").toFile());

			keyStore.load(keyStream, "changeit".toCharArray());
			kmf.init(keyStore, "changeit".toCharArray());

			SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
			sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
			return sslContext;
	}
	
	private void assertStringContainsAllKeysAndValues(String in) {
	    //System.out.println(in);
		Assert.assertTrue(in, in.contains(AuditMessage.FORMAT_VERSION));
		Assert.assertTrue(in, in.contains(AuditMessage.CATEGORY));
		Assert.assertTrue(in, in.contains(AuditMessage.FORMAT_VERSION));
		Assert.assertTrue(in, in.contains(AuditMessage.REMOTE_ADDRESS));
		Assert.assertTrue(in, in.contains(AuditMessage.ORIGIN));
		Assert.assertTrue(in, in.contains(AuditMessage.REQUEST_LAYER));
		Assert.assertTrue(in, in.contains(AuditMessage.TRANSPORT_REQUEST_TYPE));
		Assert.assertTrue(in, in.contains(AuditMessage.UTC_TIMESTAMP));
		Assert.assertTrue(in, in.contains(Category.FAILED_LOGIN.name()));
		Assert.assertTrue(in, in.contains("FAILED_LOGIN"));
		Assert.assertTrue(in, in.contains("John Doe"));
		Assert.assertTrue(in, in.contains("8.8.8.8"));
		//Assert.assertTrue(in, in.contains("CN=kirk,OU=client,O=client,L=test,C=DE"));
	}
}
