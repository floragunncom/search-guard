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

package com.floragunn.searchguard.enterprise.auditlog.impl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auditlog.helper.MockRestRequest;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.Category;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.legacy.test.AbstractSGUnitTest;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.UserInformation;
import com.google.common.base.Joiner;

public class DisabledCategoriesTest {

    ClusterService cs = mock(ClusterService.class);
    DiscoveryNode dn = mock(DiscoveryNode.class);
    ConfigurationRepository configurationRepository = mock(ConfigurationRepository.class);

    @Before
    public void setup() {
        when(dn.getHostAddress()).thenReturn("hostaddress");
        when(dn.getId()).thenReturn("hostaddress");
        when(dn.getHostName()).thenReturn("hostaddress");
        when(cs.localNode()).thenReturn(dn);
        when(cs.getClusterName()).thenReturn(new ClusterName("cname"));
        TestAuditlogImpl.clear();
    }

	@Test
	public void completetlyInvalidConfigurationTest() throws Exception {
		Builder settingsBuilder = Settings.builder();
		settingsBuilder.put("searchguard.audit.type", TestAuditlogImpl.class.getName());
		settingsBuilder.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "nonexistent");
        settingsBuilder.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "nonexistent");
		AuditLogImpl auditLog = new AuditLogImpl(settingsBuilder.build(), null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository);
		logAll(auditLog);

		auditLog.close();

		String result = TestAuditlogImpl.sb.toString();
		Assert.assertTrue(categoriesPresentInLog(result, filterComplianceCategories(Category.values())));

	}

	@Test
	public void invalidConfigurationTest() {
		Builder settingsBuilder  = Settings.builder();
		settingsBuilder.put("searchguard.audit.type", "debug");
		settingsBuilder.put("searchguard.audit.config.disabled_categories", "nonexistant, bad_headers");
		AuditLog auditLog = new AuditLogImpl(settingsBuilder.build(), null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository);
		logAll(auditLog);
		String result = TestAuditlogImpl.sb.toString();
		Assert.assertFalse(categoriesPresentInLog(result, Category.BAD_HEADERS));
	}

	@Test
	public void enableAllCategoryTest() throws Exception {
		final Builder settingsBuilder  = Settings.builder();

		settingsBuilder.put("searchguard.audit.type", TestAuditlogImpl.class.getName());
		settingsBuilder.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "NONE");
        settingsBuilder.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "NONE");

		// we use the debug output, no ES client is needed. Also, we
		// do not need to close.
		AuditLogImpl auditLog = new AuditLogImpl(settingsBuilder.build(), null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository);

		logAll(auditLog);

		// we're using the ExecutorService in AuditLogImpl, so we need to wait
		// until all tasks are finished before we can check the result
		auditLog.close();

		String result = TestAuditlogImpl.sb.toString();

		Assert.assertTrue(Category.values()+"#"+result, categoriesPresentInLog(result, filterComplianceCategories(Category.values())));

		Assert.assertThat(result, containsString("testuser.rest.succeededlogin"));
		Assert.assertThat(result, containsString("testuser.rest.failedlogin"));
		Assert.assertThat(result, containsString("privilege.missing"));
		Assert.assertThat(result, containsString("action.indexattempt"));
		Assert.assertThat(result, containsString("action.transport.ssl"));
		Assert.assertThat(result, containsString("action.success"));
		Assert.assertThat(result, containsString("Empty"));
		Assert.assertThat(result, containsString("putted-composable-template-name"));
		Assert.assertThat(result, containsString("putted-legacy-template-name"));
		Assert.assertThat(result, containsString("deleted-template-name"));
		Assert.assertThat(result, containsString("created-index-name"));
		Assert.assertThat(result, containsString("deleted-index-name"));
		Assert.assertThat(result, containsString("index-with-updated-settings"));
		Assert.assertThat(result, containsString("index-with-updated-mappings"));
		Assert.assertThat(result, containsString("testuser.kibana.succeededlogin"));
		Assert.assertThat(result, containsString("testuser.kibana.succeededlogout"));
	}

	@Test
	public void disableSingleCategoryTest() throws Exception {
		for (Category category : Category.values()) {
		    TestAuditlogImpl.clear();
			checkCategoriesDisabled(category);
		}
	}

	@Test
	public void disableAllCategoryTest() throws Exception{
		checkCategoriesDisabled(Category.values());
	}

	@Test
	public void disableSomeCategoryTest() throws Exception{
		checkCategoriesDisabled(Category.AUTHENTICATED, Category.BAD_HEADERS, Category.FAILED_LOGIN, Category.INDEX_TEMPLATE_WRITE);
	}

	/*@After
	public void restoreOut() {
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
	}*/

	protected void checkCategoriesDisabled(Category ... disabledCategories) throws Exception {

		List<String> categoryNames = new LinkedList<>();
		for (Category category : disabledCategories) {
			categoryNames.add(category.name().toLowerCase());
		}
		String disabledCategoriesString = Joiner.on(",").join(categoryNames);

		Builder settingsBuilder  = Settings.builder();
		settingsBuilder.put("searchguard.audit.type", TestAuditlogImpl.class.getName());
		settingsBuilder.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, disabledCategoriesString);
        settingsBuilder.put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, disabledCategoriesString);


		// we use the debug output, no ES client is needed. Also, we
		// do not need to close.
		AuditLog auditLog = new AuditLogImpl(settingsBuilder.build(), null, null, AbstractSGUnitTest.MOCK_POOL, null, cs, configurationRepository);

		logAll(auditLog);

		auditLog.close();

		String result = TestAuditlogImpl.sb.toString();

		List<Category> allButDisablesCategories = new LinkedList<>(Arrays.asList(Category.values()));
		allButDisablesCategories.removeAll(Arrays.asList(disabledCategories));

		//System.out.println(result+"###"+disabledCategoriesString);
		Assert.assertFalse(categoriesPresentInLog(result, disabledCategories));
		Assert.assertTrue(categoriesPresentInLog(result, filterComplianceCategories(allButDisablesCategories.toArray(new Category[] {}))));
	}

	protected boolean categoriesPresentInLog(String result, Category ... categories) {
		// since we're logging a JSON structure, whitespaces between keys and
		// values must not matter
		result = result.replaceAll(" ", "");
		for (Category category : categories) {
			if(!result.contains("\""+AuditMessage.CATEGORY+"\":\""+category.name()+"\"")) {
				//System.out.println("MISSING: "+category.name());
			    return false;
			}
		}
		return true;
	}

	protected void logAll(AuditLog auditLog) {
		//11 requests
	    logRestFailedLogin(auditLog);
		logRestBadHeaders(auditLog);
		logRestSSLException(auditLog);
		logRestSucceededLogin(auditLog);

		logMissingPrivileges(auditLog);
		logSgIndexAttempt(auditLog);
		logAuthenticatedRequest(auditLog);

		logTransportSSLException(auditLog);
		logTransportBadHeaders(auditLog);
		logTransportFailedLogin(auditLog);
		logTransportSucceededLogin(auditLog);
		
		logBlockedIp(auditLog);
		logBlockedUser(auditLog);
		logComposableIndexTemplatePutted(auditLog);
		logLegacyIndexTemplatePutted(auditLog);
		logIndexTemplateDeleted(auditLog);
		logIndexCreated(auditLog);
		logIndicesDeleted(auditLog);
		logIndexSettingsUpdated(auditLog);
		logIndexMappingsUpdated(auditLog);
		logSucceededKibanaLogin(auditLog);
		logSucceededKibanaLogout(auditLog);
    }

	 protected void logRestSucceededLogin(AuditLog auditLog) {
	     auditLog.logSucceededLogin(UserInformation.forName("testuser.rest.succeededlogin"), false, UserInformation.forName("testuser.rest.succeededlogin"), new MockRestRequest());
	 }

	 protected void logTransportSucceededLogin(AuditLog auditLog) {
	     auditLog.logSucceededLogin(UserInformation.forName("testuser.transport.succeededlogin"), false, UserInformation.forName("testuser.transport.succeededlogin"), new TransportRequest.Empty(), "test/action", new Task(0, "x", "ac", "", null, null));
	 }


    protected void logRestFailedLogin(AuditLog auditLog) {
    	auditLog.logFailedLogin(UserInformation.forName("testuser.rest.failedlogin"), false, UserInformation.forName("testuser.rest.failedlogin"), new MockRestRequest());
    }

    protected void logTransportFailedLogin(AuditLog auditLog) {
    	auditLog.logFailedLogin(UserInformation.forName("testuser.transport.failedlogin"), false, UserInformation.forName("testuser.transport.failedlogin"), new TransportRequest.Empty(), null);
    }

    protected void logMissingPrivileges(AuditLog auditLog) {
    	auditLog.logMissingPrivileges("privilege.missing", new TransportRequest.Empty(), null);
    }

    protected void logTransportBadHeaders(AuditLog auditLog) {
    	auditLog.logBadHeaders(new TransportRequest.Empty(),"action", null);
    }

    protected void logRestBadHeaders(AuditLog auditLog) {
    	auditLog.logBadHeaders(new MockRestRequest());
    }

    protected void logSgIndexAttempt(AuditLog auditLog) {
    	auditLog.logSgIndexAttempt(new TransportRequest.Empty(), "action.indexattempt", null);
    }

    protected void logRestSSLException(AuditLog auditLog) {
    	auditLog.logSSLException(new MockRestRequest(), new Exception());
    }

    protected void logTransportSSLException(AuditLog auditLog) {
    	auditLog.logSSLException(new TransportRequest.Empty(), new Exception(), "action.transport.ssl", null);
    }

    protected void logAuthenticatedRequest(AuditLog auditLog) {
    	auditLog.logGrantedPrivileges("action.success", new TransportRequest.Empty(), null);
    }
    
    

    protected void logBlockedIp(AuditLog auditLog) {
        auditLog.logBlockedIp(new MockRestRequest(), new InetSocketAddress(1234));
    }
    
    protected void logBlockedUser(AuditLog auditLog) {
        auditLog.logBlockedUser(UserInformation.forName("horst"), false, UserInformation.forName("horst"), new MockRestRequest());
    }

	protected void logComposableIndexTemplatePutted(AuditLog auditLog) {
		ComposableIndexTemplate indexTemplate = new ComposableIndexTemplate.Builder().build();
		auditLog.logIndexTemplatePutted("putted-composable-template-name", null,
				indexTemplate, "action.index-template-putted", new TransportRequest.Empty()
		);
	}

	protected void logLegacyIndexTemplatePutted(AuditLog auditLog) {
		IndexTemplateMetadata indexTemplate = IndexTemplateMetadata.builder("putted-legacy-template-name")
				.patterns(Collections.singletonList("index-pattern")).build();
		auditLog.logIndexTemplatePutted("putted-composable-template-name", null,
				indexTemplate, "action.index-template-putted", new TransportRequest.Empty()
		);
	}

	protected void logIndexTemplateDeleted(AuditLog auditLog) {
		auditLog.logIndexTemplateDeleted(Collections.singletonList("deleted-template-name"),
				"action.index-template-deleted", new TransportRequest.Empty()
		);
	}

	protected void logIndexCreated(AuditLog auditLog) {
		auditLog.logIndexCreated("created-index-name", "action.index-created", new TransportRequest.Empty());
	}

	protected void logIndicesDeleted(AuditLog auditLog) {
		auditLog.logIndicesDeleted(Collections.singletonList("deleted-index-name"),
				"action.index-deleted", new TransportRequest.Empty()
		);
	}

	protected void logIndexSettingsUpdated(AuditLog auditLog) {
		Settings settings = Settings.EMPTY;
		auditLog.logIndexSettingsUpdated(Collections.singletonList("index-with-updated-settings"),
				"index-with-updated-settings", new TransportRequest.Empty()
		);
	}

	protected void logIndexMappingsUpdated(AuditLog auditLog) {
		try {
			auditLog.logIndexMappingsUpdated(Collections.singletonList("index-with-updated-mappings"),
					"index-with-updated-mappings", new TransportRequest.Empty()
			);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    protected void logSucceededKibanaLogin(AuditLog auditLog) {
        auditLog.logSucceededKibanaLogin(UserInformation.forName("testuser.kibana.succeededlogin"));
    }

    protected void logSucceededKibanaLogout(AuditLog auditLog) {
        auditLog.logSucceededKibanaLogout(UserInformation.forName("testuser.kibana.succeededlogout"));
    }


    private static final Category[] filterComplianceCategories(Category[] cats) {
        List<Category> retval = new ArrayList<AuditMessage.Category>();
        for(Category c: cats) {
            if(!c.toString().startsWith("COMPLIANCE")) {
                retval.add(c);
            }
        }
        return retval.toArray(new Category[0]);
    }

}
