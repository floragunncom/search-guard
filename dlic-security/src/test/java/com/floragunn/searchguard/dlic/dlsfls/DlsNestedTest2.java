package com.floragunn.searchguard.dlic.dlsfls;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
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
            restClient.putJson("/nestedindex/_doc/5?refresh=true", "{ \"field1\": \"c\",\"field2\": [    {      \"field3\": \"1\"    },    {      \"field3\": \"3\"    }  ]}");         
            restClient.putJson("/nestedindex/_doc/6?refresh=true", "{ \"field1\": \"d\",\"field2\": [    {      \"field3\": \"2\"    },    {      \"field3\": \"3\"    }  ]}");         

        }
    }
    
    @Test
    public void testWildcardQueryWithTopLevelDLS() throws Exception {
        // Wildcard query on nestedindex
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("dls_nested_top_level", "password")) {
            // DLS for role filters out parents where field1 equals "a", so we are left with 4 documents
            // with ids 2,3,5 and 6
            SearchResponse searchResponse = client.search(new SearchRequest("nestedindex"), RequestOptions.DEFAULT);
            Assert.assertEquals(searchResponse.toString(), 4, searchResponse.getHits().getTotalHits().value);            
            searchResponse.getHits().forEach(hit -> { Assert.assertTrue("Unexpected doc id: "+ hit.getId(), (hit.getId().equals("2") || hit.getId().equals("3") || hit.getId().equals("5") || hit.getId().equals("6")));});
        }
    }

    @Test
    public void testToplevelQueryWithTopLevelDLS() throws Exception {
        // Search request on top-level documents
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("dls_nested_top_level", "password")) {
            SearchRequest request = new SearchRequest("nestedindex");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
            sourceBuilder.query(QueryBuilders.termQuery("field1", "c")); 
            request.source(sourceBuilder);
            // Query should return only 1 document
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            Assert.assertEquals(searchResponse.toString(), 1, searchResponse.getHits().getTotalHits().value);            
            Assert.assertTrue(searchResponse.getHits().getAt(0).getId().equals("5"));
        }
    }

    @Test
    public void testNestedQueryWithTopLevelDLS() throws Exception {
        // Search request on nested documents with top-level DLS enabled
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("dls_nested_top_level", "password")) {
            SearchRequest request = new SearchRequest("nestedindex");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
            TermQueryBuilder nestedBuilder = QueryBuilders.termQuery("field2.field3", "1");
            sourceBuilder.query(QueryBuilders.nestedQuery("field2", nestedBuilder, ScoreMode.None));             
            request.source(sourceBuilder);
            // Query should return 2 documents. We have three documents where the nested field
            // "field3" has value "1" as in the query. 
            // However, from these documents, the document with id "4" must be filtered out by
            // the DLS query. DLS filters documents where "field1" equals "a"
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.getHits().getTotalHits().value);            
            searchResponse.getHits().forEach(hit -> { Assert.assertTrue("Unexpected doc id: "+ hit.getId(), (hit.getId().equals("2") || hit.getId().equals("5")));});
        }
    }

    @Test
    public void testWildcardQueryWithNestedlDLS() throws Exception {
        // Wildcard query on nestedindex
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("dls_nested", "password")) {
            // DLS for role only returns children where field3 equals "1", so we are left with 3 documents
            // with ids 2,4 and 5
            SearchResponse searchResponse = client.search(new SearchRequest("nestedindex"), RequestOptions.DEFAULT);
            Assert.assertEquals(searchResponse.toString(), 3, searchResponse.getHits().getTotalHits().value);            
            searchResponse.getHits().forEach(hit -> { Assert.assertTrue("Unexpected doc id: "+ hit.getId(), (hit.getId().equals("2") || hit.getId().equals("4") || hit.getId().equals("5")));});
        }
    }

    @Test    
    @Ignore
    public void testNestedQueryWithNestedlDLS() throws Exception {
        // Search request on nested documents with top-level DLS enabled
        try (RestHighLevelClient client = cluster.getRestHighLevelClient("dls_nested_top_level", "password")) {
            SearchRequest request = new SearchRequest("nestedindex");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
            TermQueryBuilder nestedBuilder = QueryBuilders.termQuery("field2.field3", "3");
            sourceBuilder.query(QueryBuilders.nestedQuery("field2", nestedBuilder, ScoreMode.None));             
            request.source(sourceBuilder);
            // We query for documents where nested field3 has value "3", which means document 5 and 6
            // The DLS query should return only documents where nested field3 has value "1"
            // Which would leave us with only document with ID 5
            // However, the test returns doc id 5 and doc ID 6
            // TODO: Clarify why this test fails
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            Assert.assertEquals(searchResponse.toString(), 2, searchResponse.getHits().getTotalHits().value);            
            searchResponse.getHits().forEach(hit -> { Assert.assertTrue("Unexpected doc id: "+ hit.getId(), (hit.getId().equals("5")));});
        }
    }    
}
