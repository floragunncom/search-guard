package com.floragunn.searchguard.dlic.dlsfls;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;

public class DlsNestedTest2 {
    
    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().resources("dlsnested").build();
            

    @BeforeClass
    public static void setupTestData() throws Exception {

        try (GenericRestClient restClient = cluster.getRestClient("admin", "password")) {

            String mapping = "{\n" + 
                    "  \"mappings\": {\n" + 
                    "    \"properties\": {\n" + 
                    "      \"field1\": {\n" + 
                    "        \"type\": \"text\"\n" + 
                    "      },\n" + 
                    "      \"field2\": {\n" + 
                    "        \"type\": \"nested\",\n" + 
                    "        \"properties\": {\n" + 
                    "          \"field3\": {\n" + 
                    "            \"type\": \"text\"\n" + 
                    "          }\n" + 
                    "        }\n" + 
                    "      }\n" + 
                    "    }\n" + 
                    "  }\n" + 
                    "}";
            
            restClient.putJson("/nestedindex", mapping);
            
            restClient.putJson("/nestedindex/_doc/1?refresh=true", "{ \"field1\": \"a\"}");
            restClient.putJson("/nestedindex/_doc/2?refresh=true", "{ \"field1\": \"b\", \"field2\": [    {      \"field3\": \"1\"    },    {      \"field3\": \"2\"    }  ]}");
            restClient.putJson("/nestedindex/_doc/3?refresh=true", "{ \"field1\": \"b\"}");
            restClient.putJson("/nestedindex/_doc/4?refresh=true", "{ \"field1\": \"a\",\"field2\": [    {      \"field3\": \"1\"    },    {      \"field3\": \"2\"    }  ]}");
            restClient.putJson("/nestedindex/_doc/5?refresh=true", "{ \"field1\": \"c\",\"field2\": [    {      \"field3\": \"1\"    },    {      \"field3\": \"2\"    }  ]}");         
        }
    }

    @Test
    public void testQueryOnParentDocs() throws Exception {

        try (RestHighLevelClient client = cluster.getRestHighLevelClient("dls_nested", "password")) {

            // DLS for role filters our parents where field1 equals "a", so we are left with 3 documents
            SearchResponse searchResponse = client.search(new SearchRequest("nestedindex"), RequestOptions.DEFAULT);
            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.getHits().getTotalHits().value);            
            searchResponse.getHits().forEach(hit -> { Assert.assertTrue("Unexpected doc id: "+ hit.getId(), (hit.getId().equals("2") || hit.getId().equals("3") || hit.getId().equals("5")));});
        }


    }


}
