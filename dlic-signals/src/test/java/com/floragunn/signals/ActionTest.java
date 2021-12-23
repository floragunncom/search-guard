package com.floragunn.signals;

import java.net.URI;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.config.elements.InlineMustacheTemplate;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.enterprise.watch.action.handlers.jira.JiraAccount;
import com.floragunn.signals.enterprise.watch.action.handlers.jira.JiraAction;
import com.floragunn.signals.enterprise.watch.action.handlers.jira.JiraIssueConfig;
import com.floragunn.signals.enterprise.watch.action.handlers.pagerduty.PagerDutyAccount;
import com.floragunn.signals.enterprise.watch.action.handlers.pagerduty.PagerDutyAction;
import com.floragunn.signals.enterprise.watch.action.handlers.pagerduty.PagerDutyEventConfig;
import com.floragunn.signals.execution.ExecutionEnvironment;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.init.WatchInitializationService;

@PowerMockIgnore({ "javax.script.*", "javax.crypto.*", "javax.management.*", "sun.security.*", "java.security.*", "javax.net.ssl.*", "javax.net.*",
        "javax.security.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest(AccountRegistry.class)
public class ActionTest {

    private static NamedXContentRegistry xContentRegistry;
    private static ScriptService scriptService;

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().nodeSettings("signals.enabled", true)
            .resources("sg_config/signals").enterpriseModulesEnabled().enableModule(SignalsModule.class).build();

    @BeforeClass
    public static void setupTestData() {

        try (Client client = cluster.getInternalNodeClient()) {
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "x", "b", "y"))
                    .actionGet();
            client.index(new IndexRequest("testsource").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(XContentType.JSON, "a", "xx", "b", "yy"))
                    .actionGet();
        }
    }

    @BeforeClass
    public static void setupDependencies() {
        xContentRegistry = cluster.getInjectable(NamedXContentRegistry.class);
        scriptService = cluster.getInjectable(ScriptService.class);
    }

    @Test
    public void testPagerDutyAction() throws Exception {

        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/mockerduty")) {

            PagerDutyAccount account = new PagerDutyAccount("bla");
            account.setUri(webhookProvider.getUri());

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_account", PagerDutyAccount.class)).thenReturn(account);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("component", "stuff");
            runtimeData.put("summary", "kaputt");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));
            WatchInitializationService watchInitializationService = new WatchInitializationService(accountRegistry, scriptService);

            PagerDutyEventConfig eventConfig = new PagerDutyEventConfig();
            eventConfig.setDedupKey(InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "my_key"));

            PagerDutyEventConfig.Payload payload = new PagerDutyEventConfig.Payload();
            payload.setComponent(InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "{{data.component}}"));
            payload.setEventClass(InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "my_class"));
            payload.setSource(InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "hell"));
            payload.setSummary(InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "{{data.summary}}"));

            eventConfig.setPayload(payload);

            PagerDutyAction pagerDutyAction = new PagerDutyAction("test_account", eventConfig, false);

            pagerDutyAction.execute(ctx);

            Assert.assertEquals(
                    "{\"routing_key\":\"bla\",\"event_action\":\"trigger\",\"dedup_key\":\"my_key\",\"payload\":{\"summary\":\"kaputt\",\"source\":\"hell\",\"severity\":\"error\",\"component\":\"stuff\",\"group\":null,\"class\":\"my_class\",\"custom_details\":null}}",
                    webhookProvider.getLastRequestBody());
        }
    }

    @Test
    public void testJiraAction() throws Exception {

        try (Client client = cluster.getInternalNodeClient(); MockWebserviceProvider webhookProvider = new MockWebserviceProvider("/mockra/*")) {

            JiraAccount account = new JiraAccount(new URI(webhookProvider.getUri()), "x", "y");

            AccountRegistry accountRegistry = Mockito.mock(AccountRegistry.class);
            Mockito.when(accountRegistry.lookupAccount("test_account", JiraAccount.class)).thenReturn(account);

            NestedValueMap runtimeData = new NestedValueMap();
            runtimeData.put("path", "hook");
            runtimeData.put("component", "stuff");
            runtimeData.put("summary", "kaputt");

            WatchExecutionContext ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry,
                    ExecutionEnvironment.SCHEDULED, ActionInvocationType.ALERT, new WatchExecutionContextData(runtimeData));
            WatchInitializationService watchInitializationService = new WatchInitializationService(accountRegistry, scriptService);

            JiraIssueConfig jiraIssueConfig = new JiraIssueConfig("bug",
                    InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "Look: {{data.summary}}"),
                    InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "Indeed: {{data.summary}}"));

            jiraIssueConfig.setComponentTemplate(InlineMustacheTemplate.parse(watchInitializationService.getScriptService(), "{{data.component}}"));

            JiraAction jiraAction = new JiraAction("test_account", "Project", jiraIssueConfig);

            jiraAction.execute(ctx);

            Assert.assertEquals(
                    "{\"fields\":{\"project\":{\"key\":\"Project\"},\"summary\":\"Look: kaputt\",\"description\":\"Indeed: kaputt\",\"issuetype\":{\"name\":\"bug\"},\"components\":[{\"name\":\"stuff\"}]}}",
                    webhookProvider.getLastRequestBody());
        }
    }
}
