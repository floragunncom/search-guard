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
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;

public class EncryptedIndicesIntegrationTest {

    private static final String PRIVATE_KEY_1 = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCxvBUb0DhwEwkksqI8PJfGLiyP2xSUGwb70jnikZ0jTTNRI1DGdWwAbx9fYXIAByYXssZOX6eLARhfvI20LYM/fF5IfFz9JkHRE9QMao1UgjV+z5+9MN6JHc41E/I5aHijeL6nTKfyFXlhCk/ZbZSuDVtESvoOKt5tcazrOesHqI0RHmcPzPAs3W1E/SVZ5apqls858cjm0W9p9M9ifzZr8nxA/avJshEQ8hEfP86ZvIUb3M/qKNdAktFgjCue7ESTJviua3xxtWcI9z9LIfbHw1Hnl/Cv4ASbQY5nXGD5DG3W0MlWq+5mom6eeehVlnGssjqR3y9wI7ub/z7ZCqlRAgMBAAECggEAFuj7YJUvvTSa9F3JX1HhL4TSri1rgubT+OBhoUSrWHpST9ZpSlem9wxb4yfUsc+6F4puGPqoBlk7CtYrfurx9NxDa/0J4IDOsZRofDw84QSSwDijqteyi8Kpirp6Oe+vQ0UkcDzHlkMx3PIfFlQTevcSSWSPxIU+nCVvyHd0Bg26slhAQbZhKZWvXkfOY4yOHPzhcarYD2s+Q26iBcYOH38B9IAyT4moTZkS+or0kcwdE1ZHrhylaGAomVD/ZMPMzd+CtCsGTMvUXAuOgzBYsER3sQa/3J9LYWuRZ3pzxscDW0xIVcMoUYD/mbKfXi1UII3sUR1vAhgdX9CBMjdMpQKBgQD0+qU66NCi7t4fgsxvcva7bFKOOrI41MCaTYadCrENwU0ZTNTMAyYHLcTkyE0DO9gY5lXp9zlfC8Mv1G08VuA+oQ0hcx7BmIsY39wPFtQMZOlzl5E+T0XHPDR2MiYPehLDZA0rbyWAhvOTIhc/ODP6JnRFzU+5W+vEnpN9fXUVRQKBgQC5uwCkol0mOCkriAQ/40iNcY7fLp6oPNC94LZOZGELjO/IvpAE7b0RT8wKCiUmB0kiUnD3GrEeXqDjVJIg8v49jFSwSxLVzx+HDWskuhH2hPfyQLWVihtIBjGrTT1umoFTKd8TYj+o1S5CbhFOxvtwgUc5tzYk7ooepQ7vHhkGnQKBgEqHXnE3lxGanhT0FAHr9cg7Qjpm/QVxJE9NOqDYOdk3b5880phmdNFGSVpY3aUYNbwNhyGwxtF1oKISfFEZFQu4r2f3v+mh4N9ma2pjxYsnwCYcfGF6eH4OgN9cjluzBbZP3/nQzJX3eG7QtkXTcWyu+jyqI5D+uBGPNMu+uToJAoGAD8bZzCJapUd5/8+jBMZKwHEYAM9V/NaFqMtw0QHn2HJVYAkH9NM5D0JnA6dO9ocB6F92ZxcmWn0RT548d34MqK/F9d+6rtzUQcWbB1ii8/zhjvt+MUC1Bo44I+QAxudq+uSApYXgAHhzYIM3BykR7MGeikGM4OA+bVH6DcfRumUCgYBTz+N7Dya9toExylJlI80cGznqHJ6xy3Bl7fr+NwO/IsN/hu4PLAj8/t6Dpu1D1kLeNPpc2ykmNUbf9X0/ZkhXrhYjbNzzlBv7N40Q0kHqXVDfpBpuP84qbHKjhfoFBcbjfLRkjRkcZ+TmtM1GYUEEv/8fsYFyK9kvCiUD4PMU8w==";

    private static final String PRIVATE_KEY_2 = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCFjm/TMBvUIPhWGI9a9p0+8FW47kdn0ZzQnpG217x7EpF4K3cfzpEpw0EQ47qtenSkuvEb1aJX3lwq1heVOVgWP77Ku3ZoD7kLi8kyOl04Z7MGQygdVanE6A6ZIMxZBzH5MQoOCU1xq4aTNIhccdigIJeI8Pq+EHARJyO6edfG1jw85eABctuy4newvrNYvZW/BI7tuh3ssUNWVgI9lEEhkH/xH2yVcTxbdfe1gng9ASJCcuBxIpfQtXvF1g5pedR5CVNtGCStp2F2aCy5aeLaz2+aYcjqxWABHBMMvK/GtVvoMO6mi4twPsjC4R2qEGhe+nD39UBsRBy0mDAlKpGzAgMBAAECggEAD988GeiLPhwG91Bm5QQO7vZn8ZjbwpJOJRrbLVQZMbUktZfQyeZmKQTr3CJ8QnadmyAeXFT4vKGP6YVU+yZ7+fTsazJ5IWfQ8XbU29PE1Vm5lGJxx36xNNXxOjCEKIZq4Xb6/20Kl5ovXZouJHJhMLJ+38bnvaO9dcRlghlN0/Lg9JS7m+sC7Rtz/vQoqG7ojLuN0gH5ApmwGtG7qvgziMo78ZdqhRV6PKO72zrKKYC45qtbOmTN4MDHvzSBNtviAQ3FFaOlmukPuVwadmFs/hOfh6L19aaUCEdm8zUniotmt0PZ0xjmHL80UWbnAwM1jP/zIesJ1EFhIDj0M8rSPQKBgQC7e2F7xifQtwhiW9XRFH4uuR4cbk+iLoxd6OXYw3pKynqO+WaIFEd+FFxDLUcK9cN3Yzat89vwRSk8dRDeMfS/eaFylSt127jGvw7xKBYV+YL0gBcS2sCvxUAmSHdn1LSuFABNX5a2Ny2tNbV6ZfqMEdKCDEgr7/Kxdoxnwx0QxwKBgQC2XdX3ULhBVCiJxeh8WnLPYJ159PqH84AZ0gXD/MKH2fYM+k6ba54WwdWI+2I/qbhapHT7U7ZNK4QUUsiPYIVM2BoW3dmLjLzFRIbHS1HyiAoMlrOCwBzTHMzQZauYDpFTqe9IlPrfuEJke/TQrV+3DdFFF0mgdwTvzLHbD5mjtQKBgQCeAhRcvtrbmwcj7oY8GmtmcXohOA9Bfr2qgBkHIWi2FARK74MsePrwFbTUoRpY8Fx3CFUTMo1Q6NkiLP+0ZKIDpj0dVv8z66TFTE0JjmFez9VAv5uytk7jVPkFytln1usYM581lrRsigCjFLsIl14cIwEpvbQt46LFUkZvRRAADQKBgQCNoZ7VLnmLMyMwrOV7/nsAF9b1qo8QGsq4QuZ7achi3aI8PgHirtfecLe4ZRPOwa8Npn+72S3SDSPM7OYahCnCnmrUq4OS38CTrD7IdPS12XWEhV3xA+bfBpUCnJOByn6PbYEK67lTGrVleOePUbbuYerPL+DcoNLSsjTHHvyBqQKBgFoaasK4nd/kMz2urQ8X3swAvmd9hNAQQJNKEeGe1zomOvcjbspYUgqX/4jTF5Rzxka+4jjWebe6/0lPTLnL0VjbRxFinH04u18qIUr5pqwo5ws5Ioa9MLCSW4DjnkufSrOF0BWeWWiIX9516q2PhzrTVB0Ftyp9s26llwRf84X0";

    private static final String PUBLIC_KEY_2 = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhY5v0zAb1CD4VhiPWvadPvBVuO5HZ9Gc0J6Rtte8exKReCt3H86RKcNBEOO6rXp0pLrxG9WiV95cKtYXlTlYFj++yrt2aA+5C4vJMjpdOGezBkMoHVWpxOgOmSDMWQcx+TEKDglNcauGkzSIXHHYoCCXiPD6vhBwEScjunnXxtY8POXgAXLbsuJ3sL6zWL2VvwSO7bod7LFDVlYCPZRBIZB/8R9slXE8W3X3tYJ4PQEiQnLgcSKX0LV7xdYOaXnUeQlTbRgkradhdmgsuWni2s9vmmHI6sVgARwTDLyvxrVb6DDupouLcD7IwuEdqhBoXvpw9/VAbEQctJgwJSqRswIDAQAB";

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





        }

        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin", new BasicHeader("x-osec-pk", PRIVATE_KEY_1))) {

            for(int i=0;i<50;i++) {

                restClient.postJson("the_encrypted_index/_doc?refresh=true",
                        "{\n" +
                                "   \"full_name\":\"Mister Spock no"+i+"\",\n" +
                                "   \"credit_card_number\":\"1701"+i+"\",\n" +
                                "   \"age\":"+(100+i)+",\n" +
                                "   \"remarks\":\"great brain "+i+"\"\n" +
                                "}");

                restClient.postJson("the_encrypted_index/_doc?refresh=true",
                        "{\n" +
                                "   \"full_name\":\"Captain Kirk no"+i+"\",\n" +
                                "   \"credit_card_number\":\"1234"+i+"\",\n" +
                                "   \"age\":"+(45+i)+",\n" +
                                "   \"remarks\":\"take care "+i+"\"\n" +
                                "}");


            }

            Thread.sleep(5*1000);

            for(int i=50;i<100;i++) {

                restClient.postJson("the_encrypted_index/_doc?refresh=true",
                        "{\n" +
                                "   \"full_name\":\"Mister Spock no"+i+"\",\n" +
                                "   \"credit_card_number\":\"1701"+i+"\",\n" +
                                "   \"age\":"+(100+i)+",\n" +
                                "   \"remarks\":\"great brain "+i+"\"\n" +
                                "}");

                restClient.postJson("the_encrypted_index/_doc?refresh=true",
                        "{\n" +
                                "   \"full_name\":\"Captain Kirk no"+i+"\",\n" +
                                "   \"credit_card_number\":\"1234"+i+"\",\n" +
                                "   \"age\":"+(45+i)+",\n" +
                                "   \"remarks\":\"take care "+i+"\"\n" +
                                "}");

            }


            restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Doctor McCoy\",\n" +
                            "   \"credit_card_number\":\"9999\",\n" +
                            "   \"age\":70,\n" +
                            "   \"remarks\":\"also called pille\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            Thread.sleep(5*1000);

        }

            for(int i=0;i<20;i++)
        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin", new BasicHeader("x-osec-pk", PRIVATE_KEY_1))) {
            GenericRestClient.HttpResponse result = restClient.get("_search?pretty&size=1000");
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

            result = restClient.postJson("_search?pretty&size=1000", query);
            System.out.println(result.getBody());
            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("take"));
            Assert.assertTrue(result.getBody().contains("pille"));




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
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            System.out.println(result.getBody());


            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Captain Kirk\",\n" +
                            "   \"credit_card_number\":\"1234\",\n" +
                            "   \"age\":45,\n" +
                            "   \"remarks\":\"take care\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            System.out.println(result.getBody());

            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Doctor McCoy\",\n" +
                            "   \"credit_card_number\":\"9999\",\n" +
                            "   \"age\":70,\n" +
                            "   \"remarks\":\"also called pille\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            System.out.println(result.getBody());


            result = restClient.get("_search?pretty",new BasicHeader("x-osec-pk", PRIVATE_KEY_1));
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

            result = restClient.postJson("_search?pretty", query,new BasicHeader("x-osec-pk", PRIVATE_KEY_1));
            System.out.println(result.getBody());
            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("take"));
            Assert.assertTrue(result.getBody().contains("pille"));
        }

    }

    @Test
    public void noHeaderTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            client.admin().indices().create(
                    new CreateIndexRequest("the_encrypted_index").source(
                            FileHelper.loadFile("encrypted_indices/index.json")
                            , XContentType.JSON)).actionGet();
        }


        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {

            GenericRestClient.HttpResponse result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Mister Spock\",\n" +
                            "   \"credit_card_number\":\"1701\",\n" +
                            "   \"age\":100,\n" +
                            "   \"remarks\":\"great brain\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk1", PRIVATE_KEY_1));

            System.out.println(result.getBody());


            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Captain Kirk\",\n" +
                            "   \"credit_card_number\":\"1234\",\n" +
                            "   \"age\":45,\n" +
                            "   \"remarks\":\"take care\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            System.out.println(result.getBody());

            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Doctor McCoy\",\n" +
                            "   \"credit_card_number\":\"9999\",\n" +
                            "   \"age\":70,\n" +
                            "   \"remarks\":\"also called pille\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            System.out.println(result.getBody());


            result = restClient.get("_search?pretty");
            System.out.println(result.getBody());

            Assert.assertFalse(result.getBody().contains("Kirk"));
            Assert.assertFalse(result.getBody().contains("Doctor"));
            Assert.assertFalse(result.getBody().contains("Spock"));


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
            Assert.assertFalse(result.getBody().contains("Kirk"));
            Assert.assertFalse(result.getBody().contains("Doctor"));
            Assert.assertFalse(result.getBody().contains("take"));
            Assert.assertFalse(result.getBody().contains("pille"));
        }

    }

    @Test
    public void anotherKeyTest() throws Exception {

        try (Client client = cluster.getInternalNodeClient()) {

            client.admin().indices().create(
                    new CreateIndexRequest("the_encrypted_index").source(
                            FileHelper.loadFile("encrypted_indices/index.json")
                            , XContentType.JSON)).actionGet();

        }


        try (GenericRestClient restClient = cluster.getRestClient("admin", "admin")) {

            GenericRestClient.HttpResponse result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Mister Spock\",\n" +
                            "   \"credit_card_number\":\"1701\",\n" +
                            "   \"age\":100,\n" +
                            "   \"remarks\":\"great brain\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Captain Kirk\",\n" +
                            "   \"credit_card_number\":\"1234\",\n" +
                            "   \"age\":45,\n" +
                            "   \"remarks\":\"take care\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            result = restClient.postJson("the_encrypted_index/_doc?refresh=true",
                    "{\n" +
                            "   \"full_name\":\"Doctor McCoy\",\n" +
                            "   \"credit_card_number\":\"9999\",\n" +
                            "   \"age\":70,\n" +
                            "   \"remarks\":\"also called pille\"\n" +
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));


            result = restClient.get("_search?pretty",new BasicHeader("x-osec-pk", PRIVATE_KEY_2));

            Assert.assertFalse(result.getBody().contains("Kirk"));
            Assert.assertFalse(result.getBody().contains("Doctor"));
            Assert.assertFalse(result.getBody().contains("Spock"));
            System.out.println(result.getBody());


            String query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"match\": {\n" +
                    "      \"full_name\": {\n" +
                    "        \"query\": \"KIRK DOCtor\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            result = restClient.postJson("_search?pretty", query,new BasicHeader("x-osec-pk", PRIVATE_KEY_2));
            System.out.println(result.getBody());
            Assert.assertFalse(result.getBody().contains("Kirk"));
            Assert.assertFalse(result.getBody().contains("Doctor"));
            Assert.assertFalse(result.getBody().contains("take"));
            Assert.assertFalse(result.getBody().contains("pille"));


            result = restClient.postJson("_searchguard/api/encrypted_indices/the_encrypted_index/_add_keys",
                    "{\n" +
                            "   \"public_key\": " +
                            "\""+PUBLIC_KEY_2+"\"" +
                            ""+
                            "}",
                    new BasicHeader("x-osec-pk", PRIVATE_KEY_1));

            System.out.println(result.getBody());

            result = restClient.get("_search?pretty",new BasicHeader("x-osec-pk", PRIVATE_KEY_2));

            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("Spock"));
            System.out.println(result.getBody());

            query = "{\n" +
                    "  \"query\": {\n" +
                    "    \"match\": {\n" +
                    "      \"full_name\": {\n" +
                    "        \"query\": \"KIRK DOCtor\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            result = restClient.postJson("_search?pretty", query,new BasicHeader("x-osec-pk", PRIVATE_KEY_2));
            System.out.println(result.getBody());
            Assert.assertTrue(result.getBody().contains("Kirk"));
            Assert.assertTrue(result.getBody().contains("Doctor"));
            Assert.assertTrue(result.getBody().contains("take"));
            Assert.assertTrue(result.getBody().contains("pille"));

        }

    }
}
