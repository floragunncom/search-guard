package com.floragunn.searchguard.enterprise.encrypted_indices;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.searchguard.authtoken.AuthTokenModule;
import com.floragunn.searchguard.authtoken.RequestedPrivileges;
import com.floragunn.searchguard.authtoken.api.CreateAuthTokenRequest;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.cluster.LocalEsCluster;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.http.message.BasicHeader;
import org.apache.lucene.codecs.Codec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Map;

public class EncryptedIndicesIntegrationTest {

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder()
            .nodeSettings("searchguard.restapi.roles_enabled.0", "sg_admin")
            .resources("encrypted_indices")
            .sslEnabled()
            .enterpriseModulesEnabled()
            //.enableModule(AuthTokenModule.class)
            .build();

    @BeforeClass
    public static void setupTestData() throws Exception{

        System.out.println(Codec.availableCodecs());



    }


    @Test
    public void advTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            client.admin().indices().create(
                    new CreateIndexRequest("the_encrypted_index").source(
                            FileHelper.loadFile("encrypted_indices/index.json")
                            , XContentType.JSON)).actionGet();


            for(int i=0;i<50;i++) {

                client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(XContentType.JSON, "full_name",
                                "Mister Spock no"+i, "credit_card_number", "1701"+i, "age", 100+i, "remarks", "great brain "+i)).actionGet();

                client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(XContentType.JSON, "full_name",
                                "Captain Kirkn o"+i, "credit_card_number", "1234"+i, "age", 45+i, "remarks", "take care "+i)).actionGet();

            }

            Thread.sleep(10*1000);

            for(int i=50;i<100;i++) {

                client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(XContentType.JSON, "full_name",
                                "Mister Spock no"+i, "credit_card_number", "1701"+i, "age", 100+i, "remarks", "great brain "+i)).actionGet();

                client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(XContentType.JSON, "full_name",
                                "Captain Kirk no"+i, "credit_card_number", "1234"+i, "age", 45+i, "remarks", "take care "+i)).actionGet();

            }


            client.index(new IndexRequest("unencrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Doctor McCoy","credit_card_number","9999","age", 70, "remarks", "also called pille")).actionGet();

            Thread.sleep(10*1000);


        }


        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {
            GenericRestClient.HttpResponse result = restClient.get("_search?pretty&size=1000");
            System.out.println(result.getBody());

            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("Spock"));

            Assert.assertFalse(result.getBody().contains("kriK"));
            Assert.assertFalse(result.getBody().contains("rotcoD"));
            Assert.assertFalse(result.getBody().contains("kcopS"));
            Assert.assertFalse(result.getBody().contains("erac"));

            String query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"match\": {\n" +
                    "      \"full_name\": {\n" +
                    "        \"query\": \"KIRK DOCtor\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            result = restClient.postJson("_search?pretty&size=1000", query);
            System.out.println(result.getBody());
            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("take"));
            Assert.assertTrue(result.getBody().contains("pille"));

            Assert.assertFalse(result.getBody().contains("kriK"));
            Assert.assertFalse(result.getBody().contains("rotcoD"));
            Assert.assertFalse(result.getBody().contains("kcopS"));
            Assert.assertFalse(result.getBody().contains("erac"));


            query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"term\": {\n" +
                    "      \"credit_card_number\": {\n" +
                    "        \"value\": \"123481\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            result = restClient.postJson("_search?pretty&size=1000", query);
            System.out.println(result.getBody());
            Assert.assertTrue(result.getBody().contains("Kirk no81"));
            Assert.assertTrue(result.getBody().contains("take care 81"));
            Assert.assertTrue(result.getBody().contains("123481"));
        }

    }


    @Test
    public void basicTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            client.admin().indices().create(
                    new CreateIndexRequest("the_encrypted_index").source(
                            FileHelper.loadFile("encrypted_indices/index.json")
                            , XContentType.JSON)).actionGet();


            client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Mister Spock","credit_card_number","1701","age", 100, "remarks", "great brain")).actionGet();

            client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Captain Kirk","credit_card_number","1234","age", 45, "remarks", "take care")).actionGet();

            client.index(new IndexRequest("unencrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Doctor McCoy","credit_card_number","9999","age", 70, "remarks", "also called pille")).actionGet();
        }


        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {
            GenericRestClient.HttpResponse result = restClient.get("_search?pretty");
            System.out.println(result.getBody());

            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("Spock"));


            String query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"match\": {\n" +
                    "      \"full_name\": {\n" +
                    "        \"query\": \"KIRK DOCtor\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            result = restClient.postJson("_search?pretty", query);
            System.out.println(result.getBody());
            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("take"));
            Assert.assertTrue(result.getBody().contains("pille"));
        }

    }
}
