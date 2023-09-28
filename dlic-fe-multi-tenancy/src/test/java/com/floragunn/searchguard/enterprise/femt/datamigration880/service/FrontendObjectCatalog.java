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

    public void insertIndexPattern(String indexName, String...titles) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(IMMEDIATE);
        for(String currentName : titles) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            String indexPatternId = "index-pattern::" + currentName;
            indexRequest.id(indexPatternId);
            String spaceJson = indexPatternForTitle(currentName);
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

    private String indexPatternForTitle(String title) {
        return """
            {
            	"index-pattern": {
            		"fieldFormatMap": "{}",
            		"runtimeFieldMap": "{}",
            		"fieldAttrs": "{}",
            		"sourceFilters": "[]",
            		"typeMeta": "{}",
            		"timeFieldName": "@timestamp",
            		"name": ".alerts-security.alerts-admin_space_2,apm-*-transaction*,auditbeat-*,endgame-*,filebeat-*,logs-*,packetbeat-*,traces-apm*,winlogbeat-*,-*elastic-cloud-logs-*",
            		"title": "%%%TITLE%%%",
            		"fields": "[]",
            		"allowNoIndex": true
            	},
            	"references": [],
            	"updated_at": "2023-07-21T14:34:38.651Z",
            	"managed": false,
            	"typeMigrationVersion": "8.0.0",
            	"coreMigrationVersion": "8.8.0",
            	"created_at": "2023-07-21T14:34:38.651Z",
            	"type": "index-pattern",
            	"namespaces": [
            		"admin_space_2"
            	]
            }
            """.replace("%%%TITLE%%%", title);
    }
}
