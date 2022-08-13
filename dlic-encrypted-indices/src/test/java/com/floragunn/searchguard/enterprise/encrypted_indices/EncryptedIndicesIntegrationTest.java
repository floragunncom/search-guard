package com.floragunn.searchguard.enterprise.encrypted_indices;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.helper.cluster.FileHelper;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.message.BasicHeader;
import org.apache.lucene.codecs.Codec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;

public class EncryptedIndicesIntegrationTest {

    private static final String PK = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCxvBUb0DhwEwkksqI8PJfGLiyP2xSUGwb70jnikZ0jTTNRI1DGdWwAbx9fYXIAByYXssZOX6eLARhfvI20LYM/fF5IfFz9JkHRE9QMao1UgjV+z5+9MN6JHc41E/I5aHijeL6nTKfyFXlhCk/ZbZSuDVtESvoOKt5tcazrOesHqI0RHmcPzPAs3W1E/SVZ5apqls858cjm0W9p9M9ifzZr8nxA/avJshEQ8hEfP86ZvIUb3M/qKNdAktFgjCue7ESTJviua3xxtWcI9z9LIfbHw1Hnl/Cv4ASbQY5nXGD5DG3W0MlWq+5mom6eeehVlnGssjqR3y9wI7ub/z7ZCqlRAgMBAAECggEAFuj7YJUvvTSa9F3JX1HhL4TSri1rgubT+OBhoUSrWHpST9ZpSlem9wxb4yfUsc+6F4puGPqoBlk7CtYrfurx9NxDa/0J4IDOsZRofDw84QSSwDijqteyi8Kpirp6Oe+vQ0UkcDzHlkMx3PIfFlQTevcSSWSPxIU+nCVvyHd0Bg26slhAQbZhKZWvXkfOY4yOHPzhcarYD2s+Q26iBcYOH38B9IAyT4moTZkS+or0kcwdE1ZHrhylaGAomVD/ZMPMzd+CtCsGTMvUXAuOgzBYsER3sQa/3J9LYWuRZ3pzxscDW0xIVcMoUYD/mbKfXi1UII3sUR1vAhgdX9CBMjdMpQKBgQD0+qU66NCi7t4fgsxvcva7bFKOOrI41MCaTYadCrENwU0ZTNTMAyYHLcTkyE0DO9gY5lXp9zlfC8Mv1G08VuA+oQ0hcx7BmIsY39wPFtQMZOlzl5E+T0XHPDR2MiYPehLDZA0rbyWAhvOTIhc/ODP6JnRFzU+5W+vEnpN9fXUVRQKBgQC5uwCkol0mOCkriAQ/40iNcY7fLp6oPNC94LZOZGELjO/IvpAE7b0RT8wKCiUmB0kiUnD3GrEeXqDjVJIg8v49jFSwSxLVzx+HDWskuhH2hPfyQLWVihtIBjGrTT1umoFTKd8TYj+o1S5CbhFOxvtwgUc5tzYk7ooepQ7vHhkGnQKBgEqHXnE3lxGanhT0FAHr9cg7Qjpm/QVxJE9NOqDYOdk3b5880phmdNFGSVpY3aUYNbwNhyGwxtF1oKISfFEZFQu4r2f3v+mh4N9ma2pjxYsnwCYcfGF6eH4OgN9cjluzBbZP3/nQzJX3eG7QtkXTcWyu+jyqI5D+uBGPNMu+uToJAoGAD8bZzCJapUd5/8+jBMZKwHEYAM9V/NaFqMtw0QHn2HJVYAkH9NM5D0JnA6dO9ocB6F92ZxcmWn0RT548d34MqK/F9d+6rtzUQcWbB1ii8/zhjvt+MUC1Bo44I+QAxudq+uSApYXgAHhzYIM3BykR7MGeikGM4OA+bVH6DcfRumUCgYBTz+N7Dya9toExylJlI80cGznqHJ6xy3Bl7fr+NwO/IsN/hu4PLAj8/t6Dpu1D1kLeNPpc2ykmNUbf9X0/ZkhXrhYjbNzzlBv7N40Q0kHqXVDfpBpuP84qbHKjhfoFBcbjfLRkjRkcZ+TmtM1GYUEEv/8fsYFyK9kvCiUD4PMU8w==";

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


            /*client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Mister Spock","credit_card_number","1701","age", 100, "remarks", "great brain")).actionGet();

            client.index(new IndexRequest("the_encrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Captain Kirk","credit_card_number","1234","age", 45, "remarks", "take care")).actionGet();

            client.index(new IndexRequest("unencrypted_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(XContentType.JSON, "full_name",
                            "Doctor McCoy","credit_card_number","9999","age", 70, "remarks", "also called pille")).actionGet();*/
        }


        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {

            GenericRestClient.HttpResponse result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Mister Spock\",\n" +
                            "   \"credit_card_number\":\"1701\",\n" +
                            "   \"age\":100,\n" +
                            "   \"remarks\":\"great brain\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PK));

            System.out.println(result.getBody());


            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Captain Kirk\",\n" +
                            "   \"credit_card_number\":\"1234\",\n" +
                            "   \"age\":45,\n" +
                            "   \"remarks\":\"take care\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PK));

            System.out.println(result.getBody());

            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Doctor McCoy\",\n" +
                            "   \"credit_card_number\":\"9999\",\n" +
                            "   \"age\":70,\n" +
                            "   \"remarks\":\"also called pille\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PK));

            System.out.println(result.getBody());


            result = restClient.get("_search?pretty",new BasicHeader("x-osec-pk", PK));
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
