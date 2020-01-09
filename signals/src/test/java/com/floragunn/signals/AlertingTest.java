package com.floragunn.signals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import com.floragunn.signals.watch.WatchBuilder;
import com.floragunn.signals.watch.Watch;

public class AlertingTest extends SingleClusterTest {

    protected RestHelper rh = null;

    @Override
    protected String getResourceFolder() {
        return "sg_config/signals";
    }

    @Override
    protected final void setup(Settings nodeOverride) throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put(nodeOverride);

        builder.put("searchguard.ssl.http.enabled", true)
                .put("searchguard.ssl.http.keystore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(getResourceFolder() + "/node-0-keystore.jks"))
                .put("searchguard.ssl.http.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(getResourceFolder() + "/truststore.jks"));

        setup(builder.build(), ClusterConfiguration.SINGLENODE);
        rh = restHelper();

    }

    @Test
    @Ignore
    public void test() throws Exception {
        Header auth = basicAuth("uhura", "uhura");

        Settings settings = Settings.builder().put("signals.enabled", true).build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            tc.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

            Watch watch = new WatchBuilder("test").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").transform("trigger.triggered_time").as("ttt").then().index("testsink")
                    .name("testsink").build();

            HttpResponse response = rh.executePutRequest("/_signals/watch/_main/test1", watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(1500 * 1000);
        }
    }

    @Test
    public void testTenants() throws Exception {
        Header auth = basicAuth("uhura", "uhura");

        Settings settings = Settings.builder().put("signals.enabled", true).build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            tc.index(new IndexRequest("testsource").source(XContentType.JSON, "key1", "val1", "key2", "val2")).actionGet();

            Watch watch = new WatchBuilder("test").cronTrigger("*/2 * * * * ?").search("testsource").query("{\"match_all\" : {} }").as("testsearch")
                    .put("{\"bla\": {\"blub\": 42}}").as("teststatic").then().index("testsink").name("testsink").build();

            HttpResponse response = rh.executePutRequest("/_signals/watch/_main/test1", watch.toJson(), auth);
            Assert.assertEquals(response.getBody(), HttpStatus.SC_CREATED, response.getStatusCode());

            Thread.sleep(15 * 1000);
            //TODO: check active tenants

            tc.index(new IndexRequest("searchguard").id("tenants").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("tenants",
                    FileHelper.readYamlContent("sg_config/signals/sg_roles_tenants2.yml"))).actionGet();
            ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[] { "tenants" })).actionGet();
            Assert.assertFalse(cur.hasFailures());
            Assert.assertEquals(clusterInfo.numNodes, cur.getNodes().size());

            //Thread.sleep(1500 * 1000);
            //TODO: check active tenants after we added a few and removed some
        }
    }

    @Override
    protected Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {
        Settings.Builder builder = super.minimumSearchGuardSettingsBuilder(node, sslOnly);

        builder.put("node.attr.node_index", node);

        if (node == 1) {
            builder.put("node.attr.exec_watches", "true");
        }

        return builder;
    }

    private static Header basicAuth(String username, String password) {
        return new BasicHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
    }
}
