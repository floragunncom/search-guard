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

package com.floragunn.searchguard.legacy;

import java.util.Arrays;
import java.util.Collection;

import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper;
import com.floragunn.searchguard.legacy.test.SingleClusterTest;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;

@RunWith(Parameterized.class)
public class SnapshotRestoreTests extends SingleClusterTest {

    @ClassRule 
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();
    
    @Parameters
    public static Collection<ClusterConfiguration> data() {
        return Arrays.asList(new ClusterConfiguration[] {     
                ClusterConfiguration.DEFAULT
           });
    }
    
    @Parameter
    public ClusterConfiguration currentClusterConfig;

    @Ignore // TODO
    @Test
    public void testSnapshotEnableSgIndexRestore() throws Exception {
    
        final Settings settings = Settings.builder()
                .putList("path.repo", repositoryPath.getRoot().getAbsolutePath())
                .put("searchguard.check_snapshot_restore_write_privileges", false)
                .put("searchguard.unsupported.restore.sgindex.enabled", true)
                .build();
    
        setup(settings, currentClusterConfig);
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "vulcangov").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/vulcangov")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "vulcangov", "vulcangov_1").indices("vulcangov").includeGlobalState(true).waitForCompletion(true)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "searchguard").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/searchguard")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "searchguard", "searchguard_1").indices("searchguard").includeGlobalState(false).waitForCompletion(true)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "all").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/all")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "all", "all_1").indices("*").includeGlobalState(false).waitForCompletion(true)).actionGet();
    
        RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/vulcangov", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/vulcangov/vulcangov_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_$1\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"include_global_state\": true, \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_with_global_state_$1\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // worf not allowed to restore vulcangov index
        HttpResponse response = rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","", encodeBasicHeader("worf", "worf"));
        Assert.assertEquals(response.getBody(), HttpStatus.SC_FORBIDDEN, response.getStatusCode());
        // Try to restore vulcangov index as searchguard index, not possible since SG index is open
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"indices\": \"vulcangov\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore searchguard index.
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/searchguard", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/searchguard/searchguard_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // 500 because SG index is open
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","{ \"indices\": \"searchguard\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard_copy\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore all indices.
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/all", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/all/all_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // 500 because SG index is open
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore vulcangov index as searchguard index -> 500 because SG index is open
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","{ \"indices\": \"vulcangov\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index. Delete searchguard_copy first, was created in test above
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeDeleteRequest("searchguard_copy", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","{ \"indices\": \"searchguard\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard_copy\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore a unknown snapshot
        Assert.assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, rh.executePostRequest("_snapshot/all/unknown-snapshot/_restore?wait_for_completion=true", "", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        
        // close and restore SG index
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("searchguard/_close", "", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("searchguard/_open", "", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    }
    
    @Test
    public void testSnapshot() throws Exception {
    
        final Settings settings = Settings.builder()
                .putList("path.repo", repositoryPath.getRoot().getAbsolutePath())
                .put("searchguard.check_snapshot_restore_write_privileges", false)
                .build();
    
        setup(settings, currentClusterConfig);
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "vulcangov").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/vulcangov")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "vulcangov", "vulcangov_1").indices("vulcangov").includeGlobalState(true).waitForCompletion(true)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "searchguard").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/searchguard")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "searchguard", "searchguard_1").indices("searchguard").includeGlobalState(false).waitForCompletion(true)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "all").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/all")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "all", "all_1").indices("*").includeGlobalState(false).waitForCompletion(true)).actionGet();

        RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/vulcangov", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/vulcangov/vulcangov_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_$1\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"include_global_state\": true, \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_with_global_state_$1\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","", encodeBasicHeader("worf", "worf")).getStatusCode());
        // Try to restore vulcangov index as searchguard index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"indices\": \"vulcangov\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore searchguard index.
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/searchguard", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/searchguard/searchguard_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","{ \"indices\": \"searchguard\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard_copy\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore all indices.
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/all", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/all/all_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","{ \"indices\": \"vulcangov\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","{ \"indices\": \"searchguard\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard_copy\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore a unknown snapshot
        
        //System.out.println("---------------------------------------------------------------");
        
        Assert.assertEquals(500, rh.executePostRequest("_snapshot/all/unknown-snapshot/_restore?wait_for_completion=true", "", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Assert.assertEquals(HttpStatus.SC_FORBIDDEN, executePostRequest("_snapshot/all/unknown-snapshot/_restore?wait_for_completion=true","{ \"indices\": \"the-unknown-index\" }", encodeBasicHeader("nagilum", "nagilum"))).getStatusCode());
    }

    @Test
    public void testSnapshotCheckWritePrivileges() throws Exception {
    
        final Settings settings = Settings.builder()
                .putList("path.repo", repositoryPath.getRoot().getAbsolutePath())
                .build();
    
        setup(settings, currentClusterConfig);
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("vulcangov").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "vulcangov").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/vulcangov")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "vulcangov", "vulcangov_1").indices("vulcangov").includeGlobalState(true).waitForCompletion(true)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "searchguard").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/searchguard")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "searchguard", "searchguard_1").indices("searchguard").includeGlobalState(false).waitForCompletion(true)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "all").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/all")).get();
        tc.admin().cluster().createSnapshot(new CreateSnapshotRequest(TimeValue.timeValueSeconds(30), "all", "all_1").indices("*").includeGlobalState(false).waitForCompletion(true)).actionGet();

        ConfigUpdateResponse cur = tc.execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(new String[]{"config","roles","rolesmapping","internalusers","actiongroups"})).actionGet();
        Assert.assertFalse(cur.hasFailures());
        Assert.assertEquals(currentClusterConfig.getNodes(), cur.getNodes().size());
        //System.out.println(cur.getNodesMap());

        RestHelper rh = nonSslRestHelper();
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/vulcangov", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/vulcangov/vulcangov_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_$1\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"include_global_state\": true, \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_with_global_state_$1\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","", encodeBasicHeader("worf", "worf")).getStatusCode());
        // Try to restore vulcangov index as searchguard index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"indices\": \"vulcangov\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore searchguard index.
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/searchguard", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/searchguard/searchguard_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/searchguard/searchguard_1/_restore?wait_for_completion=true","{ \"indices\": \"searchguard\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard_copy\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore all indices.
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/all", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executeGetRequest("_snapshot/all/all_1", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","{ \"indices\": \"vulcangov\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
        // Try to restore searchguard index as serchguard_copy index
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/all/all_1/_restore?wait_for_completion=true","{ \"indices\": \"searchguard\", \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"searchguard_copy\" }", encodeBasicHeader("nagilum", "nagilum")).getStatusCode());
    
        // Try to restore a unknown snapshot
        HttpResponse response = rh.executePostRequest("_snapshot/all/unknown-snapshot/_restore?wait_for_completion=true", "", encodeBasicHeader("nagilum", "nagilum"));
        Assert.assertEquals(response.getBody(), 500, response.getStatusCode());
    
        // Tests snapshot with write permissions (OK)
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"$1_restore_1\" }", encodeBasicHeader("restoreuser", "restoreuser")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"$1_restore_2a\" }", encodeBasicHeader("restoreuser", "restoreuser")).getStatusCode());
    
        // Test snapshot with write permissions (OK)
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"$1_no_restore_1\" }", encodeBasicHeader("restoreuser", "restoreuser")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"$1_no_restore_2\" }", encodeBasicHeader("restoreuser", "restoreuser")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"$1_no_restore_3\" }", encodeBasicHeader("restoreuser", "restoreuser")).getStatusCode());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/vulcangov/vulcangov_1/_restore?wait_for_completion=true","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"$1_no_restore_4\" }", encodeBasicHeader("restoreuser", "restoreuser")).getStatusCode());
    }

    @Test
    public void testSnapshotRestore() throws Exception {
    
        final Settings settings = Settings.builder()
                .putList("path.repo", repositoryPath.getRoot().getAbsolutePath())
                .build();
    
        setup(Settings.EMPTY, new DynamicSgConfig().setSgActionGroups("sg_action_groups_packaged.yml"), settings, true, currentClusterConfig);
    
        Client tc = getPrivilegedInternalNodeClient();
        tc.index(new IndexRequest("testsnap1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("testsnap2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("testsnap3").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("testsnap4").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("testsnap5").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();
        tc.index(new IndexRequest("testsnap6").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source("{\"content\":1}", XContentType.JSON)).actionGet();

        tc.admin().cluster().preparePutRepository(TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30), "bckrepo").setType("fs").setSettings(Settings.builder().put("location", repositoryPath.getRoot().getAbsolutePath() + "/bckrepo")).get();

        RestHelper rh = nonSslRestHelper();        
        String putSnapshot =
        "{"+
          "\"indices\": \"testsnap1\","+
          "\"ignore_unavailable\": false,"+
          "\"include_global_state\": false"+
        "}";
        
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePutRequest("_snapshot/bckrepo/"+putSnapshot.hashCode()+"?wait_for_completion=true&pretty", putSnapshot, encodeBasicHeader("snapresuser", "nagilum")).getStatusCode()); 
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePostRequest("_snapshot/bckrepo/"+putSnapshot.hashCode()+"/_restore?wait_for_completion=true&pretty","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_$1\" }", encodeBasicHeader("snapresuser", "nagilum")).getStatusCode());
        
        putSnapshot =
        "{"+
          "\"indices\": \"searchguard\","+
          "\"ignore_unavailable\": false,"+
          "\"include_global_state\": false"+
        "}";
                
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePutRequest("_snapshot/bckrepo/"+putSnapshot.hashCode()+"?wait_for_completion=true&pretty", putSnapshot, encodeBasicHeader("snapresuser", "nagilum")).getStatusCode()); 
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/bckrepo/"+putSnapshot.hashCode()+"/_restore?wait_for_completion=true&pretty","{ \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_$1\" }", encodeBasicHeader("snapresuser", "nagilum")).getStatusCode());
              
        putSnapshot =
        "{"+
          "\"indices\": \"testsnap2\","+
          "\"ignore_unavailable\": false,"+
          "\"include_global_state\": true"+
        "}";
                        
        Assert.assertEquals(HttpStatus.SC_OK, rh.executePutRequest("_snapshot/bckrepo/"+putSnapshot.hashCode()+"?wait_for_completion=true&pretty", putSnapshot, encodeBasicHeader("snapresuser", "nagilum")).getStatusCode()); 
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, rh.executePostRequest("_snapshot/bckrepo/"+putSnapshot.hashCode()+"/_restore?wait_for_completion=true&pretty","{ \"include_global_state\": true, \"rename_pattern\": \"(.+)\", \"rename_replacement\": \"restored_index_$1\" }", encodeBasicHeader("snapresuser", "nagilum")).getStatusCode());
    }

 
}
