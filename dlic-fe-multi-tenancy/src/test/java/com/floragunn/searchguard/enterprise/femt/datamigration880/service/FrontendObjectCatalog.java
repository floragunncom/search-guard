package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.Objects;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FrontendObjectCatalog {

    private final PrivilegedConfigClient client;

    public FrontendObjectCatalog(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public void insertSpace(String indexName, String...names) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(IMMEDIATE);
        for(String currentName : names) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            String spaceId = "space:" + currentName;
            indexRequest.id(spaceId);
            String spaceJson = spaceForName(currentName);
            indexRequest.source(spaceJson, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        assertThat(response.hasFailures(), equalTo(false));
    }

    private String spaceForName(String spaceName) {
        return """
            {
            	"space": {
            		"name": "%%%NAME%%%",
            		"description": "This is your default space!",
            		"color": "#00bfb3",
            		"disabledFeatures": [],
            		"_reserved": true
            	},
            	"type": "space",
            	"references": [],
            	"managed": false,
            	"coreMigrationVersion": "8.7.0",
            	"typeMigrationVersion": "6.6.0",
            	"updated_at": "2023-07-20T15:21:07.913Z",
            	"created_at": "2023-07-20T15:21:07.913Z"
            }
            """.replace("%%%NAME%%%", spaceName);
    }
}
