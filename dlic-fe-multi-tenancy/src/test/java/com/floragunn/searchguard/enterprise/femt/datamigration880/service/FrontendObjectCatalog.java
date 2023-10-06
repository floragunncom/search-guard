package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FrontendObjectCatalog {

    private final PrivilegedConfigClient client;

    public FrontendObjectCatalog(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Client is required");
    }

    public ImmutableList<String> insertSpace(String indexName, String...names) {
        if(names.length == 0) {
            throw new IllegalArgumentException("Nothing to insert!");
        }
        List<String> spacesIds = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(IMMEDIATE);
        for(String currentName : names) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            String spaceId = "space:" + currentName;
            indexRequest.id(spaceId);
            String spaceJson = spaceForName(currentName);
            indexRequest.source(spaceJson, XContentType.JSON);
            bulkRequest.add(indexRequest);
            spacesIds.add(spaceId);
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        return ImmutableList.of(spacesIds);
    }

    public ImmutableList<String> insertSpace(String indexName, int countOfDocuments) {
        String[] spaceNames = generateNames(indexName, countOfDocuments, "space_no_");
        return insertSpace(indexName, spaceNames);
    }

    public ImmutableList<String> insertIndexPattern(String indexName, String...titles) {
        if(titles.length == 0) {
            throw new IllegalArgumentException("Nothing to insert!");
        }
        List<String> indexPatternsIds = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(IMMEDIATE);
        for(String currentName : titles) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            String indexPatternId = "index-pattern::" + currentName;
            indexRequest.id(indexPatternId);
            String spaceJson = indexPatternForTitle(currentName);
            indexRequest.source(spaceJson, XContentType.JSON);
            bulkRequest.add(indexRequest);
            indexPatternsIds.add(indexPatternId);
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        return ImmutableList.of(indexPatternsIds);
    }

    public ImmutableList<String> insertDashboard(String indexName, String...names) {
        if(names.length == 0) {
            throw new IllegalArgumentException("Nothing to insert!");
        }
        List<String> dashboardsIds = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(IMMEDIATE);
        for(String currentName : names) {
            IndexRequest indexRequest = new IndexRequest(indexName);
            String dashboardId = "dashboard:" + currentName;
            indexRequest.id(dashboardId);
            String spaceJson = dashboardForName(currentName);
            indexRequest.source(spaceJson, XContentType.JSON);
            bulkRequest.add(indexRequest);
            dashboardsIds.add(dashboardId);
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        return ImmutableList.of(dashboardsIds);
    }

    public ImmutableList<String> insertDashboard(String indexName, int numberOfDocuments) {
        String[] indexPatternNames = generateNames(indexName, numberOfDocuments, "dashboard_no_");
        return insertDashboard(indexName, indexPatternNames);
    }

    public ImmutableList<String> insertIndexPattern(String indexName, int numberOfDocuments) {
        String[] indexPatternNames = generateNames(indexName, numberOfDocuments, "index_pattern_no_");
        return insertIndexPattern(indexName, indexPatternNames);
    }

    private static String[] generateNames(String indexName, int numberOfDocuments, String prefix) {
        String[] indexPatternNames = IntStream.range(0, numberOfDocuments) //
            .mapToObj(index -> indexName + prefix + index) //
            .toArray(String[]::new);
        return indexPatternNames;
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

    private String dashboardForName(String dashboardTitle) {
        return """
            	{
             	"references": [
             		{
             			"name": "1ac5404d-c8f9-4167-9026-d4e6f5220d48:indexpattern-datasource-layer-8d801c76-0f4d-4e8d-bb52-bff66851d04f",
             			"id": "5c9320e4-d42a-4056-889c-8b820c06733b",
             			"type": "index-pattern"
             		},
             		{
             			"name": "058fd824-00d2-4096-b890-5f7655041bf5:indexpattern-datasource-layer-7485bf73-e3ed-4dcf-bf42-eaaf36f169a5",
             			"id": "5c9320e4-d42a-4056-889c-8b820c06733b",
             			"type": "index-pattern"
             		},
             		{
             			"name": "3a1442bc-ee40-42cd-828a-ec63fb8cd0c9:indexpattern-datasource-layer-a0b81c91-00c5-4e5e-a5f4-6ad0b94e1cbb",
             			"id": "5c9320e4-d42a-4056-889c-8b820c06733b",
             			"type": "index-pattern"
             		}
             	],
             	"updated_at": "2023-10-06T13:09:45.990Z",
             	"managed": false,
             	"typeMigrationVersion": "8.7.0",
             	"coreMigrationVersion": "8.8.0",
             	"created_at": "2023-10-06T13:09:45.990Z",
             	"type": "dashboard",
             	"dashboard": {
             		"description": "",
             		"timeRestore": false,
             		"title": "%%%TITLE%%%",
             		"version": 1,
             		"kibanaSavedObjectMeta": {
             			"searchSourceJSON": "{\\"query\\":{\\"query\\":\\"\\",\\"language\\":\\"kuery\\"},\\"filter\\":[]}"
             		},
             		"optionsJSON": "{\\"useMargins\\":true,\\"syncColors\\":false,\\"syncCursor\\":true,\\"syncTooltips\\":false,\\"hidePanelTitles\\":false}",
             		"panelsJSON": "[{\\"version\\":\\"8.8.0\\",\\"type\\":\\"lens\\",\\"gridData\\":{\\"x\\":0,\\"y\\":0,\\"w\\":24,\\"h\\":15,\\"i\\":\\"1ac5404d-c8f9-4167-9026-d4e6f5220d48\\"},\\"panelIndex\\":\\"1ac5404d-c8f9-4167-9026-d4e6f5220d48\\",\\"embeddableConfig\\":{\\"attributes\\":{\\"title\\":\\"\\",\\"description\\":\\"\\",\\"visualizationType\\":\\"lnsXY\\",\\"type\\":\\"lens\\",\\"references\\":[{\\"type\\":\\"index-pattern\\",\\"id\\":\\"5c9320e4-d42a-4056-889c-8b820c06733b\\",\\"name\\":\\"indexpattern-datasource-layer-8d801c76-0f4d-4e8d-bb52-bff66851d04f\\"}],\\"state\\":{\\"visualization\\":{\\"title\\":\\"Empty XY chart\\",\\"legend\\":{\\"isVisible\\":true,\\"position\\":\\"right\\"},\\"valueLabels\\":\\"hide\\",\\"preferredSeriesType\\":\\"bar_stacked\\",\\"layers\\":[{\\"layerId\\":\\"8d801c76-0f4d-4e8d-bb52-bff66851d04f\\",\\"accessors\\":[\\"b815ed6a-87c5-4ed7-b24c-553dd928319b\\"],\\"position\\":\\"top\\",\\"seriesType\\":\\"bar_stacked\\",\\"showGridlines\\":false,\\"layerType\\":\\"data\\",\\"splitAccessor\\":\\"9751161a-7a7d-41e3-a0fe-7b051b00dffd\\",\\"xAccessor\\":\\"c8ab52bf-2718-4fdd-9467-d7d99a6e5650\\"}]},\\"query\\":{\\"query\\":\\"\\",\\"language\\":\\"kuery\\"},\\"filters\\":[],\\"datasourceStates\\":{\\"formBased\\":{\\"layers\\":{\\"8d801c76-0f4d-4e8d-bb52-bff66851d04f\\":{\\"columns\\":{\\"9751161a-7a7d-41e3-a0fe-7b051b00dffd\\":{\\"label\\":\\"device-id\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"range\\",\\"sourceField\\":\\"device-id\\",\\"isBucketed\\":true,\\"scale\\":\\"interval\\",\\"params\\":{\\"includeEmptyRows\\":true,\\"type\\":\\"histogram\\",\\"ranges\\":[{\\"from\\":0,\\"to\\":1000,\\"label\\":\\"\\"}],\\"maxBars\\":\\"auto\\"}},\\"c8ab52bf-2718-4fdd-9467-d7d99a6e5650\\":{\\"label\\":\\"timestamp\\",\\"dataType\\":\\"date\\",\\"operationType\\":\\"date_histogram\\",\\"sourceField\\":\\"timestamp\\",\\"isBucketed\\":true,\\"scale\\":\\"interval\\",\\"params\\":{\\"interval\\":\\"auto\\",\\"includeEmptyRows\\":true,\\"dropPartials\\":false}},\\"b815ed6a-87c5-4ed7-b24c-553dd928319b\\":{\\"label\\":\\"Median of temperature\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"median\\",\\"sourceField\\":\\"temperature\\",\\"isBucketed\\":false,\\"scale\\":\\"ratio\\",\\"params\\":{\\"emptyAsNull\\":true}}},\\"columnOrder\\":[\\"9751161a-7a7d-41e3-a0fe-7b051b00dffd\\",\\"c8ab52bf-2718-4fdd-9467-d7d99a6e5650\\",\\"b815ed6a-87c5-4ed7-b24c-553dd928319b\\"],\\"sampling\\":1,\\"incompleteColumns\\":{}}}},\\"textBased\\":{\\"layers\\":{}}},\\"internalReferences\\":[],\\"adHocDataViews\\":{}}},\\"enhancements\\":{}}},{\\"version\\":\\"8.8.0\\",\\"type\\":\\"lens\\",\\"gridData\\":{\\"x\\":24,\\"y\\":0,\\"w\\":24,\\"h\\":15,\\"i\\":\\"058fd824-00d2-4096-b890-5f7655041bf5\\"},\\"panelIndex\\":\\"058fd824-00d2-4096-b890-5f7655041bf5\\",\\"embeddableConfig\\":{\\"attributes\\":{\\"title\\":\\"\\",\\"description\\":\\"\\",\\"visualizationType\\":\\"lnsXY\\",\\"type\\":\\"lens\\",\\"references\\":[{\\"type\\":\\"index-pattern\\",\\"id\\":\\"5c9320e4-d42a-4056-889c-8b820c06733b\\",\\"name\\":\\"indexpattern-datasource-layer-7485bf73-e3ed-4dcf-bf42-eaaf36f169a5\\"}],\\"state\\":{\\"visualization\\":{\\"title\\":\\"Empty XY chart\\",\\"legend\\":{\\"isVisible\\":true,\\"position\\":\\"right\\"},\\"valueLabels\\":\\"hide\\",\\"preferredSeriesType\\":\\"line\\",\\"layers\\":[{\\"layerId\\":\\"7485bf73-e3ed-4dcf-bf42-eaaf36f169a5\\",\\"accessors\\":[\\"16799189-4827-407d-abf2-4cbc20cc43e2\\",\\"0d773342-a062-4a97-980f-33f899d34ff2\\",\\"b42915e9-fe99-4575-a66f-e482cbe69848\\"],\\"position\\":\\"top\\",\\"seriesType\\":\\"line\\",\\"showGridlines\\":false,\\"layerType\\":\\"data\\",\\"xAccessor\\":\\"2e31da67-2474-406e-a089-107b21a330df\\",\\"splitAccessor\\":\\"9487cc56-41b4-4b9f-830d-b7e26fb964df\\"}]},\\"query\\":{\\"query\\":\\"\\",\\"language\\":\\"kuery\\"},\\"filters\\":[],\\"datasourceStates\\":{\\"formBased\\":{\\"layers\\":{\\"7485bf73-e3ed-4dcf-bf42-eaaf36f169a5\\":{\\"columns\\":{\\"2e31da67-2474-406e-a089-107b21a330df\\":{\\"label\\":\\"timestamp\\",\\"dataType\\":\\"date\\",\\"operationType\\":\\"date_histogram\\",\\"sourceField\\":\\"timestamp\\",\\"isBucketed\\":true,\\"scale\\":\\"interval\\",\\"params\\":{\\"interval\\":\\"auto\\",\\"includeEmptyRows\\":true,\\"dropPartials\\":false}},\\"16799189-4827-407d-abf2-4cbc20cc43e2\\":{\\"label\\":\\"Median of temperature\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"median\\",\\"sourceField\\":\\"temperature\\",\\"isBucketed\\":false,\\"scale\\":\\"ratio\\",\\"params\\":{\\"emptyAsNull\\":true}},\\"9487cc56-41b4-4b9f-830d-b7e26fb964df\\":{\\"label\\":\\"device-id\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"range\\",\\"sourceField\\":\\"device-id\\",\\"isBucketed\\":true,\\"scale\\":\\"interval\\",\\"params\\":{\\"includeEmptyRows\\":true,\\"type\\":\\"histogram\\",\\"ranges\\":[{\\"from\\":0,\\"to\\":1000,\\"label\\":\\"\\"}],\\"maxBars\\":\\"auto\\"}},\\"0d773342-a062-4a97-980f-33f899d34ff2\\":{\\"label\\":\\"Maximum of temperature\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"max\\",\\"sourceField\\":\\"temperature\\",\\"isBucketed\\":false,\\"scale\\":\\"ratio\\",\\"params\\":{\\"emptyAsNull\\":true}},\\"b42915e9-fe99-4575-a66f-e482cbe69848\\":{\\"label\\":\\"Minimum of temperature\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"min\\",\\"sourceField\\":\\"temperature\\",\\"isBucketed\\":false,\\"scale\\":\\"ratio\\",\\"params\\":{\\"emptyAsNull\\":true}}},\\"columnOrder\\":[\\"2e31da67-2474-406e-a089-107b21a330df\\",\\"9487cc56-41b4-4b9f-830d-b7e26fb964df\\",\\"16799189-4827-407d-abf2-4cbc20cc43e2\\",\\"0d773342-a062-4a97-980f-33f899d34ff2\\",\\"b42915e9-fe99-4575-a66f-e482cbe69848\\"],\\"sampling\\":1,\\"incompleteColumns\\":{}}}},\\"textBased\\":{\\"layers\\":{}}},\\"internalReferences\\":[],\\"adHocDataViews\\":{}}},\\"enhancements\\":{}}},{\\"version\\":\\"8.8.0\\",\\"type\\":\\"lens\\",\\"gridData\\":{\\"x\\":0,\\"y\\":15,\\"w\\":24,\\"h\\":15,\\"i\\":\\"3a1442bc-ee40-42cd-828a-ec63fb8cd0c9\\"},\\"panelIndex\\":\\"3a1442bc-ee40-42cd-828a-ec63fb8cd0c9\\",\\"embeddableConfig\\":{\\"attributes\\":{\\"title\\":\\"\\",\\"description\\":\\"\\",\\"visualizationType\\":\\"lnsXY\\",\\"type\\":\\"lens\\",\\"references\\":[{\\"type\\":\\"index-pattern\\",\\"id\\":\\"5c9320e4-d42a-4056-889c-8b820c06733b\\",\\"name\\":\\"indexpattern-datasource-layer-a0b81c91-00c5-4e5e-a5f4-6ad0b94e1cbb\\"}],\\"state\\":{\\"visualization\\":{\\"title\\":\\"Empty XY chart\\",\\"legend\\":{\\"isVisible\\":true,\\"position\\":\\"right\\"},\\"valueLabels\\":\\"hide\\",\\"preferredSeriesType\\":\\"bar_stacked\\",\\"layers\\":[{\\"layerId\\":\\"a0b81c91-00c5-4e5e-a5f4-6ad0b94e1cbb\\",\\"accessors\\":[\\"6c71470f-cb62-4cfa-b2be-d93ecbfe1c04\\"],\\"position\\":\\"top\\",\\"seriesType\\":\\"bar_stacked\\",\\"showGridlines\\":false,\\"layerType\\":\\"data\\",\\"xAccessor\\":\\"1d66c4ce-100c-478d-b960-5ec32c4a97e3\\"}]},\\"query\\":{\\"query\\":\\"\\",\\"language\\":\\"kuery\\"},\\"filters\\":[],\\"datasourceStates\\":{\\"formBased\\":{\\"layers\\":{\\"a0b81c91-00c5-4e5e-a5f4-6ad0b94e1cbb\\":{\\"columns\\":{\\"1d66c4ce-100c-478d-b960-5ec32c4a97e3\\":{\\"label\\":\\"device-id\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"range\\",\\"sourceField\\":\\"device-id\\",\\"isBucketed\\":true,\\"scale\\":\\"interval\\",\\"params\\":{\\"includeEmptyRows\\":true,\\"type\\":\\"histogram\\",\\"ranges\\":[{\\"from\\":0,\\"to\\":1000,\\"label\\":\\"\\"}],\\"maxBars\\":\\"auto\\"}},\\"6c71470f-cb62-4cfa-b2be-d93ecbfe1c04\\":{\\"label\\":\\"Median of temperature\\",\\"dataType\\":\\"number\\",\\"operationType\\":\\"median\\",\\"sourceField\\":\\"temperature\\",\\"isBucketed\\":false,\\"scale\\":\\"ratio\\",\\"params\\":{\\"emptyAsNull\\":true}}},\\"columnOrder\\":[\\"1d66c4ce-100c-478d-b960-5ec32c4a97e3\\",\\"6c71470f-cb62-4cfa-b2be-d93ecbfe1c04\\"],\\"sampling\\":1,\\"incompleteColumns\\":{}}}},\\"textBased\\":{\\"layers\\":{}}},\\"internalReferences\\":[],\\"adHocDataViews\\":{}}},\\"enhancements\\":{}}}]"
             	},
             	"namespaces": [
             		"default"
             	]
             }
            """.replace("%%%TITLE%%%", dashboardTitle);
    }
}
