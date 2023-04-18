package com.floragunn.searchguard;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DataStream;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.JavaSecurityTestSetup;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.UUID;

import static com.floragunn.searchguard.test.TestSgConfig.Role.ALL_ACCESS;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.docNodeSizeEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class DataStreamTest {

    private static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    private static final TestSgConfig.User ADMIN_USER = new TestSgConfig.User("admin").roles(ALL_ACCESS);
    private static final String DATA_STREAM_INDEX_TEMPLATE_NAME = "data-stream-index-template";
    private static final String DATA_STREAM_NAME_PREFIX = "data-stream-";
    private static final String ADMIN_USER_DATA_STREAM_NAME = DATA_STREAM_NAME_PREFIX + "admin";
    private static final String ADMIN_DATA_STREAM_DOC_ID_PREFIX = "admin-";
    private static final String SHARED_DATA_STREAM_NAME = DATA_STREAM_NAME_PREFIX + "shared";
    private static final String SHARED_DATA_STREAM_DOC_ID_PREFIX = "shared-";
    private static final String FIRST_DATA_STREAM_NAME_PREFIX = DATA_STREAM_NAME_PREFIX + "first-";
    private static final String SECOND_DATA_STREAM_NAME_PREFIX = DATA_STREAM_NAME_PREFIX + "second-";
    private static final String INDEX_NAME_PREFIX = "ds-index-";
    private static final TestSgConfig.Role ROLE_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS = new TestSgConfig.Role("role-with-perms-to-data-streams")
            .indexPermissions(
                    "indices:admin/data_stream/create",
                    "indices:data/write/index",
                    "indices:admin/mapping/auto_put",
                    "indices:admin/data_stream/get",
                    "indices:admin/data_stream/delete",
                    "indices:admin/rollover",
                    "indices:admin/open"
            )
            .on(FIRST_DATA_STREAM_NAME_PREFIX + "*")
            .indexPermissions(
                    "indices:data/read/search",
                    "indices:data/read/field_caps",
                    "indices:monitor/data_stream/stats"
            )
            .on(SHARED_DATA_STREAM_NAME + "*")
            .clusterPermissions(
                    "indices:data/write/bulk",
                    "indices:data/write/reindex"
            );

    /**
     * Permissions assigned to {@link #FIRST_DATA_STREAM_NAME_PREFIX}, {@link  #SHARED_DATA_STREAM_NAME}
     * ({@value #FIRST_DATA_STREAM_NAME_PREFIX}*, {@value SHARED_DATA_STREAM_NAME}*)
     */
    private static final TestSgConfig.User USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS = new TestSgConfig.User("user-perms-assigned-to-data-streams")
            .roles(ROLE_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS);

    private static final TestSgConfig.Role ROLE_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES = new TestSgConfig.Role("role-with-perms-to-backing-indices")
            .indexPermissions(
                    "indices:admin/data_stream/create",
                    "indices:data/write/index",
                    "indices:admin/mapping/auto_put",
                    "indices:admin/data_stream/get",
                    "indices:admin/data_stream/delete",
                    "indices:admin/rollover",
                    "indices:admin/auto_create",
                    "indices:admin/open"
            )
            .on(".ds-" + SECOND_DATA_STREAM_NAME_PREFIX + "*")
            .indexPermissions(
                    "indices:data/read/search",
                    "indices:data/read/field_caps",
                    "indices:monitor/data_stream/stats"
            )
            .on(".ds-" + SHARED_DATA_STREAM_NAME + "*")
            .clusterPermissions(
                    "indices:data/write/bulk",
                    "indices:data/write/reindex"
            )
            .indexPermissions(
                    "indices:admin/data_stream/migrate",
                    "indices:admin/data_stream/modify"
            )
            .on(INDEX_NAME_PREFIX + "*");

    /**
     * Permissions assigned to {@link #SECOND_DATA_STREAM_NAME_PREFIX}, {@link #SHARED_DATA_STREAM_NAME}
     * (.ds-{@value #SECOND_DATA_STREAM_NAME_PREFIX}*, .ds-{@value #SHARED_DATA_STREAM_NAME}*)
     */
    private static final TestSgConfig.User USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES = new TestSgConfig.User("user-perms-assigned-to-backing-indices")
            .roles(ROLE_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES);

    @ClassRule
    public static JavaSecurityTestSetup javaSecurity = new JavaSecurityTestSetup();

    @ClassRule
    public static LocalCluster CLUSTER = new LocalCluster.Builder()
            .singleNode()
            .authc(AUTHC)
            .users(ADMIN_USER, USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS, USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)
            .sslEnabled()
            .enterpriseModulesEnabled()
            .plugin(DataStreamsPlugin.class)
            .build();

    @BeforeClass
    public static void setupTestData() throws Exception {
        try (RestClient lowLevelRestClient = CLUSTER.getAdminCertLowLevelRestClient()) {
            ElasticsearchClient client = new ElasticsearchClient(new RestClientTransport(lowLevelRestClient, new JacksonJsonpMapper()));

            PutIndexTemplateRequest dsIndexTemplateRequest = new PutIndexTemplateRequest.Builder()
                    .name(DATA_STREAM_INDEX_TEMPLATE_NAME)
                    .dataStream(new DataStream.Builder().build())
                    .indexPatterns(Collections.singletonList(DATA_STREAM_NAME_PREFIX + "*"))
                    .priority(500)
                    .build();
            PutIndexTemplateResponse dsIndexTemplateResponse = client.indices().putIndexTemplate(dsIndexTemplateRequest);
            assertThat(dsIndexTemplateResponse.acknowledged(), is(true));

            createDataStreams(ADMIN_USER_DATA_STREAM_NAME, SHARED_DATA_STREAM_NAME);

            createDocument(ADMIN_USER_DATA_STREAM_NAME, ADMIN_DATA_STREAM_DOC_ID_PREFIX + 1, DocNode.of("@timestamp", DateTimeFormatter.ISO_DATE.format(LocalDateTime.now()), "field", "value"));
            createDocument(ADMIN_USER_DATA_STREAM_NAME, ADMIN_DATA_STREAM_DOC_ID_PREFIX + 2, DocNode.of("@timestamp", DateTimeFormatter.ISO_DATE.format(LocalDateTime.now()), "field", "value"));

            createDocument(SHARED_DATA_STREAM_NAME, SHARED_DATA_STREAM_DOC_ID_PREFIX + 1, DocNode.of("@timestamp", DateTimeFormatter.ISO_DATE.format(LocalDateTime.now()), "field", "value"));
            createDocument(SHARED_DATA_STREAM_NAME, SHARED_DATA_STREAM_DOC_ID_PREFIX + 2, DocNode.of("@timestamp", DateTimeFormatter.ISO_DATE.format(LocalDateTime.now()), "field", "value"));
        }
    }

    @Test
    public void testCreateDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            String dataStreamToCreate = FIRST_DATA_STREAM_NAME_PREFIX + "test-create";

            //todo this user has indices:admin/data_stream/create permission assigned to data stream not to backing indices,
            // and can create data streams

            //data stream does not exist
            GenericRestClient.HttpResponse createDsResponse = restClient.put("/_data_stream/" + dataStreamToCreate);
            //todo it returns 200, 403 is expected?
            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(200));

            //data stream exists
            dataStreamToCreate = FIRST_DATA_STREAM_NAME_PREFIX + "already-exists-" + UUID.randomUUID();
            createDataStreams(dataStreamToCreate);

            createDsResponse = restClient.put("/_data_stream/" + dataStreamToCreate);
            //todo it returns 400, 403 is expected?
            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(400));

            //user has no access, data stream does not exist
            createDsResponse = restClient.put("/_data_stream/" + "something");
            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream already exists
            createDsResponse = restClient.put("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME);
            //todo it returns 400 "reason":"data_stream [data-stream-admin] already exists" instead of 403
//            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String dataStreamToCreate = SECOND_DATA_STREAM_NAME_PREFIX + "test-create";

            //todo this user has indices:admin/data_stream/create permission assigned to backing indices,
            // and cannot create data streams

            //data stream does not exist
            GenericRestClient.HttpResponse createDsResponse = restClient.put("/_data_stream/" + dataStreamToCreate);
            //todo it returns 403 "missing_permissions":"data-stream-second-test-create: indices:admin/data_stream/create"
            // seems like this permission needs to be assigned to data stream, not to backing indices
//            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(200));

            //data stream exists
            dataStreamToCreate = SECOND_DATA_STREAM_NAME_PREFIX + "already-exists-" + UUID.randomUUID();
            createDataStreams(dataStreamToCreate);

            createDsResponse = restClient.put("/_data_stream/" + dataStreamToCreate);
            //todo in previous step user cannot create data stream, in this case 400 is returned instead of 403
            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(400));

            //user has no access, data stream does not exist
            createDsResponse = restClient.put("/_data_stream/" + "something");
            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream already exists
            createDsResponse = restClient.put("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME);
            //todo it returns 400 "reason":"data_stream [data-stream-admin] already exists" instead of 403
//            assertThat(createDsResponse.getBody(), createDsResponse.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void testAddDocumentToDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            String dataStreamToAddDoc = FIRST_DATA_STREAM_NAME_PREFIX + "test-add-doc";
            createDataStreams(dataStreamToAddDoc);

            DocNode document = DocNode.of("@timestamp", ZonedDateTime.now(), "key", "value");

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            GenericRestClient.HttpResponse addDocResponse = restClient.postJson("/" + dataStreamToAddDoc + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            addDocResponse = restClient.postJson("/" + dataStreamToAddDoc + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            addDocResponse = restClient.postJson("/" + FIRST_DATA_STREAM_NAME_PREFIX + "missing" + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            addDocResponse = restClient.postJson("/" + FIRST_DATA_STREAM_NAME_PREFIX + "missing" + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access, perms are not assigned to data stream or backing indices
            addDocResponse = restClient.postJson("/" + ADMIN_USER_DATA_STREAM_NAME + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            addDocResponse = restClient.postJson("/" + ADMIN_USER_DATA_STREAM_NAME + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are not assigned to data stream or backing indices
            addDocResponse = restClient.postJson("/" + "not_existing" + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            addDocResponse = restClient.postJson("/" + "not_existing" + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String dataStreamToAddDoc = SECOND_DATA_STREAM_NAME_PREFIX + "test-add-doc";
            createDataStreams(dataStreamToAddDoc);

            DocNode document = DocNode.of("@timestamp", ZonedDateTime.now(), "key", "value");

            //data stream exists, user has access, perms are assigned to backing indices
            GenericRestClient.HttpResponse addDocResponse = restClient.postJson("/" + dataStreamToAddDoc + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(201));

            addDocResponse = restClient.postJson("/" + dataStreamToAddDoc + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(201));

            //data stream does not exist, user has access, perms are assigned to backing indices
            addDocResponse = restClient.postJson("/" + SECOND_DATA_STREAM_NAME_PREFIX + "missing" + "/_doc", document);
            //todo it returns 403 "missing_permissions":"data-stream-second-missing: indices:data/write/index"
            // it should return 201 - create data stream & add doc
//            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(201));

            addDocResponse = restClient.postJson("/" + SECOND_DATA_STREAM_NAME_PREFIX + "missing" + "/_create/" + UUID.randomUUID(), document);
            //todo it returns 403 "missing_permissions":"data-stream-second-missing: indices:data/write/index"
            // it should return 201 - create data stream & add doc
//            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(201));

            //user has no access, data stream exists
            addDocResponse = restClient.postJson("/" + ADMIN_USER_DATA_STREAM_NAME + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            addDocResponse = restClient.postJson("/" + ADMIN_USER_DATA_STREAM_NAME + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            addDocResponse = restClient.postJson("/" + "not_existing" + "/_doc", document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));

            addDocResponse = restClient.postJson("/" + "not_existing" + "/_create/" + UUID.randomUUID(), document);
            assertThat(addDocResponse.getBody(), addDocResponse.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void testBulkAddDocumentToDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            String dataStreamToAddDoc = FIRST_DATA_STREAM_NAME_PREFIX + "test-add-doc-bulk";
            createDataStreams(dataStreamToAddDoc);

            DocNode document = DocNode.of("@timestamp", ZonedDateTime.now(), "key", "value");
            DocNode create = DocNode.of("create", DocNode.EMPTY);
            String requestBody = String.format("%s\n%s\n%s\n%s\n", create.toJsonString(), document.toJsonString(), create.toJsonString(), document.toJsonString());

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            GenericRestClient.HttpResponse addDocBulkResponse = restClient.putJson("/" + dataStreamToAddDoc + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", true));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 403));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 403));

            //data stream does not exists, user has no access, perms are assigned to data stream not to backing indices
            addDocBulkResponse = restClient.putJson("/" + FIRST_DATA_STREAM_NAME_PREFIX + "missing" + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", true));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 403));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 403));

            //data stream exists, user has no access, perms are not assigned to data stream or backing indices
            addDocBulkResponse = restClient.putJson("/" + ADMIN_USER_DATA_STREAM_NAME + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", true));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 403));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 403));

            //data stream does not exist, user has no access, perms are not assigned to data stream or backing indices
            addDocBulkResponse = restClient.putJson("/" + "not_existing" + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", true));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 403));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 403));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String dataStreamToAddDoc = SECOND_DATA_STREAM_NAME_PREFIX + "test-add-doc-bulk";
            createDataStreams(dataStreamToAddDoc);

            DocNode document = DocNode.of("@timestamp", ZonedDateTime.now(), "key", "value");
            DocNode create = DocNode.of("create", DocNode.EMPTY);
            String requestBody = String.format("%s\n%s\n%s\n%s\n", create.toJsonString(), document.toJsonString(), create.toJsonString(), document.toJsonString());

            //data stream exists, user has access, perms are assigned to backing indices
            GenericRestClient.HttpResponse addDocBulkResponse = restClient.putJson("/" + dataStreamToAddDoc + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", false));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 201));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 201));

            //data stream does not exist, user has access, perms are assigned to backing indices
            addDocBulkResponse = restClient.putJson("/" + SECOND_DATA_STREAM_NAME_PREFIX + "missing" + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            //todo it returns 403 with errors - items.[*].create.error "missing_permissions":"data-stream-second-missing: indices:admin/auto_create"
            // it should return 201 - create data stream & add doc
//            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", false));
//            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 201));
//            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 201));

            //data stream exists, user has no access, perms are not assigned to data stream or backing indices
            addDocBulkResponse = restClient.putJson("/" + ADMIN_USER_DATA_STREAM_NAME + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", true));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 403));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 403));

            //data stream does not exist, user has no access, perms are not assigned to data stream or backing indices
            addDocBulkResponse = restClient.putJson("/" + "not_existing" + "/_bulk", requestBody);
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getStatusCode(), equalTo(200));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("errors", true));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[0].create.status", 403));
            assertThat(addDocBulkResponse.getBody(), addDocBulkResponse.getBodyAsDocNode(), containsValue("items.[1].create.status", 403));
        }
    }

    @Test
    public void testGetDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            String dataStreamToTestGet = FIRST_DATA_STREAM_NAME_PREFIX + "test-get";
            createDataStreams(dataStreamToTestGet);

            //user has perms assigned to data streams not to backing indices, list should be empty
            GenericRestClient.HttpResponse getDsResponse = restClient.get("/_data_stream");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //get by name

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            getDsResponse = restClient.get("/_data_stream/" + dataStreamToTestGet);
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            getDsResponse = restClient.get("/_data_stream/" + FIRST_DATA_STREAM_NAME_PREFIX + "not_existing");
            //todo it returns 404, it should return 403
//            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access, perms are not assigned to data stream or backing indices
            getDsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME);
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are not assigned to data stream or backing indices
            getDsResponse = restClient.get("/_data_stream/" + "not_existing");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(403));

            //get by prefix

            //matching data stream exists, user has perms assigned to data streams not to backing indices, list should be empty
            getDsResponse = restClient.get("/_data_stream/" + FIRST_DATA_STREAM_NAME_PREFIX + "*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream does not exist, user has perms assigned to data streams not to backing indices, list should be empty
            getDsResponse = restClient.get("/_data_stream/" + FIRST_DATA_STREAM_NAME_PREFIX + "fake*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream exists, user has perms assigned to data streams not to backing indices, list should be empty
            getDsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream does not exist, user has perms assigned to data streams not to backing indices, list should be empty
            getDsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*fake");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream exists, user has perms assigned to data streams not to backing indices, list should be empty
            getDsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream does not exist, user has perms assigned to data streams not to backing indices, list should be empty
            getDsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "fake*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String dataStreamToTestGet = SECOND_DATA_STREAM_NAME_PREFIX + "test-get";
            createDataStreams(dataStreamToTestGet);

            GenericRestClient.HttpResponse getDsResponse = restClient.get("/_data_stream");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            //todo it returns empty list, it should return at least data stream that we just created - `dataStreamToTestGet`
//            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").size(), greaterThan(0));
//            assertThat(
//                    getDsResponse.getBody(),
//                    getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").map(node -> node.getAsString("name")),
//                    everyItem(startsWith(SECOND_USER_DATA_STREAM_NAME_PREFIX))
//            );

            //get by name

            //user has access, data stream exists
            getDsResponse = restClient.get("/_data_stream/" + dataStreamToTestGet);
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(1));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode(), containsValue("data_streams.[0].name", dataStreamToTestGet));

            //user has access, data stream does not exist
            getDsResponse = restClient.get("/_data_stream/" + SECOND_DATA_STREAM_NAME_PREFIX + "not_existing");
            //todo it returns 403 "missing_permissions":"data-stream-second-not_existing: indices:admin/data_stream/get",
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            getDsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME);
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            getDsResponse = restClient.get("/_data_stream/" + "not_existing");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(403));

            //get by prefix

            //user has access, matching data stream exists
            getDsResponse = restClient.get("/_data_stream/" + SECOND_DATA_STREAM_NAME_PREFIX + "*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").size(), greaterThan(0));
            assertThat(getDsResponse.getBody(),
                    getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").map(node -> node.getAsString("name")),
                    everyItem(startsWith(SECOND_DATA_STREAM_NAME_PREFIX))
            );

            //user has access, matching data stream does not exist
            getDsResponse = restClient.get("/_data_stream/" + SECOND_DATA_STREAM_NAME_PREFIX + "fake*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //user has partial access, matching data stream exists
            getDsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            //todo it returns empty list, it should return at least data stream that we just created - `dataStreamToTestGet`
//            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").size(), greaterThan(0));
//            assertThat(getDsResponse.getBody(),
//                    getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").map(node -> node.getAsString("name")),
//                    everyItem(startsWith(SECOND_DATA_STREAM_NAME_PREFIX))
//            );

            //user has partial access, matching data stream does not exist
            getDsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*fake");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //user has no access, matching data stream exists
            getDsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //user has no access, matching data stream does not exist
            getDsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "fake*");
            assertThat(getDsResponse.getBody(), getDsResponse.getStatusCode(), equalTo(200));
            assertThat(getDsResponse.getBody(), getDsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));
        }
    }

    @Test
    public void testDeleteDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            String firstDsToRemove = FIRST_DATA_STREAM_NAME_PREFIX + "test-delete-1";
            String secondDsToRemove = FIRST_DATA_STREAM_NAME_PREFIX + "test-delete-2";
            String thirdDsToRemove = FIRST_DATA_STREAM_NAME_PREFIX + "test-delete-3";
            createDataStreams(firstDsToRemove, secondDsToRemove, thirdDsToRemove);

            //delete by name

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            GenericRestClient.HttpResponse deleteDsResponse = restClient.delete("/_data_stream/" + firstDsToRemove);
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + FIRST_DATA_STREAM_NAME_PREFIX + "not_existing");
            //todo it should return 403? it returns 404
//            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME);
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + "not_existing");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //delete by prefix

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + secondDsToRemove + "*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + FIRST_DATA_STREAM_NAME_PREFIX + "fake*");
            //todo it should return 403? it returns 200
//            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*fake");
            //todo it should return 403? it returns 200
//            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            deleteDsResponse = restClient.delete("/_data_stream/" + "fake" + "*");
            //todo it should return 403? it returns 200
//            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String firstDsToRemove = SECOND_DATA_STREAM_NAME_PREFIX + "test-delete-1";
            String secondDsToRemove = SECOND_DATA_STREAM_NAME_PREFIX + "test-delete-2";
            String thirdDsToRemove = SECOND_DATA_STREAM_NAME_PREFIX + "test-delete-3";
            createDataStreams(firstDsToRemove, secondDsToRemove, thirdDsToRemove);

            //delete by name

            //user has access, data stream exists
            GenericRestClient.HttpResponse deleteDsResponse = restClient.delete("/_data_stream/" + firstDsToRemove);
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(200));

            //user has access, data stream does not exist
            deleteDsResponse = restClient.delete("/_data_stream/" + SECOND_DATA_STREAM_NAME_PREFIX + "not_existing");
            //todo it returns 403 "missing_permissions":"data-stream-second-not_existing: indices:admin/data_stream/delete",
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            deleteDsResponse = restClient.delete("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME);
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            deleteDsResponse = restClient.delete("/_data_stream/" + "not_existing");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //delete by prefix

            //user has access, matching data stream exists
            deleteDsResponse = restClient.delete("/_data_stream/" + secondDsToRemove + "*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(200));

            //user has access, matching data stream does not exist
            deleteDsResponse = restClient.delete("/_data_stream/" + SECOND_DATA_STREAM_NAME_PREFIX + "fake*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(200));

            //user has partial access, matching data stream exists
            deleteDsResponse = restClient.delete("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*");
            //todo it returns 403, it should return 200
            // "indices:admin/data_stream/delete" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS?
//            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(200));

            //user has partial access, matching data stream does not exist
            deleteDsResponse = restClient.delete("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*fake");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(200));

            //user has no access, matching data stream exists
            deleteDsResponse = restClient.delete("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(403));

            //user has no access, matching data stream does not exist
            deleteDsResponse = restClient.delete("/_data_stream/" + "fake" + "*");
            assertThat(deleteDsResponse.getBody(), deleteDsResponse.getStatusCode(), equalTo(200));
        }
    }

    @Test
    public void testSearchDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            GenericRestClient.HttpResponse searchResponse = restClient.get("/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //search by name

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(403));


            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake" + "/_search");
            //todo it should return 403? it returns 404
//            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + "not_existing" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(403));

            //search by prefix

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*fake" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            searchResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "fake*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            GenericRestClient.HttpResponse searchResponse = restClient.get("/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(2));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[0]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[1]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));

            //search by name

            //user has access, data stream exists
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(2));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[0]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[1]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));


            //user has access, data stream does not exist
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake" + "/_search");
            //todo it returns 403 "missing_permissions":"data-stream-sharedfake: indices:data/read/search",
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            searchResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            searchResponse = restClient.get("/" + "not_existing" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(403));

            //search by prefix

            //user has access, matching data stream exists
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(2));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[0]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[1]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));

            //user has access, matching data stream does not exist;
            searchResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //user has partial access, matching data stream exists
            searchResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(2));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[0]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode(), containSubstring("hits.hits.[1]._id", SHARED_DATA_STREAM_DOC_ID_PREFIX));

            //user has partial access, matching data stream does not exist
            searchResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*fake" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //user has no access, matching data stream exists
            searchResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));

            //user has no access, matching data stream does not exist
            searchResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "fake*" + "/_search");
            assertThat(searchResponse.getBody(), searchResponse.getStatusCode(), equalTo(200));
            assertThat(searchResponse.getBody(), searchResponse.getBodyAsDocNode().getAsNode("hits").getAsListOfNodes("hits"), hasSize(0));
        }
    }

    @Test
    public void testFieldCapabilitiesDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            GenericRestClient.HttpResponse fieldCapsResponse = restClient.get("/" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //get by name

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake" + "/_field_caps?fields=@timestamp");
            //todo it should return 403? it returns 404
//            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(404));

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + "not_existing" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(403));

            //search by prefix

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*fake" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            fieldCapsResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "fake*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            GenericRestClient.HttpResponse fieldCapsResponse = restClient.get("/" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices").size(), greaterThan(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), everyItem(containsString(SHARED_DATA_STREAM_NAME)));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 1));

            //get by name

            //user has access, data stream exists
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(1));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 1));

            //user has access, data stream does not exist
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake" + "/_field_caps?fields=@timestamp");
            //todo it returns 403 "missing_permissions":"data-stream-sharedfake: indices:data/read/field_caps",
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            fieldCapsResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            fieldCapsResponse = restClient.get("/" + "not_existing" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(403));

            //search by prefix

            //user has access, matching data stream exists
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(1));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 1));

            //user has access, matching data stream does not exist;
            fieldCapsResponse = restClient.get("/" + SHARED_DATA_STREAM_NAME + "fake*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //user has partial access, matching data stream exists
            fieldCapsResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices").size(), greaterThan(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), everyItem(containsString(SHARED_DATA_STREAM_NAME)));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 1));

            //user has partial access, matching data stream does not exist
            fieldCapsResponse = restClient.get("/" + DATA_STREAM_NAME_PREFIX + "*fake" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //user has no access, matching data stream exists
            fieldCapsResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));

            //user has no access, matching data stream does not exist
            fieldCapsResponse = restClient.get("/" + ADMIN_USER_DATA_STREAM_NAME + "fake*" + "/_field_caps?fields=@timestamp");
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getStatusCode(), equalTo(200));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode().getAsListOfStrings("indices"), hasSize(0));
            assertThat(fieldCapsResponse.getBody(), fieldCapsResponse.getBodyAsDocNode(), docNodeSizeEqualTo("fields", 0));
        }
    }

    @Test
    public void testGetDataStreamStats() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            GenericRestClient.HttpResponse getStatsResponse = restClient.get("/_data_stream/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //get by name

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "fake" + "/_stats");
            //todo it should return 403? it returns 404
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + "not_existing" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(403));

            //get by prefix

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "*" + "/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "fake*" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*" + "/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*fake" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream exists, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "*" + "/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //matching data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            getStatsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "fake*" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            GenericRestClient.HttpResponse getStatsResponse = restClient.get("/_data_stream/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").size(), greaterThan(0));
//            assertThat(getStatsResponse.getBody(),
//                    getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").map(node -> node.getAsString("data_stream")),
//                    everyItem(equalTo(SHARED_DATA_STREAM_NAME))
//            );

            //get by name

            //user has access, data stream exists
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(1));

            //user has access, data stream does not exist
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "fake" + "/_stats");
            //todo it returns 403 "missing_permissions":"data-stream-sharedfake: indices:monitor/data_stream/stats",
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            getStatsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            getStatsResponse = restClient.get("/_data_stream/" + "not_existing" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(403));

            //get by prefix

            //user has access, matching data stream exists
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "*" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(1));

            //user has access, matching data stream does not exist
            getStatsResponse = restClient.get("/_data_stream/" + SHARED_DATA_STREAM_NAME + "fake*" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //user has partial access, matching data stream exists
            getStatsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*" + "/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams").size(), greaterThan(0));

            //user has partial access, matching data stream does not exist
            getStatsResponse = restClient.get("/_data_stream/" + DATA_STREAM_NAME_PREFIX + "*fake" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //user has no access, matching data stream exists
            getStatsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "*" + "/_stats");
            //todo it returns 403, it should return 200?
            // "indices:monitor/data_stream/stats" should be added to AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS &
            // AuthorizationConfig.DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT?
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
//            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));

            //user has no access, matching data stream does not exist
            getStatsResponse = restClient.get("/_data_stream/" + ADMIN_USER_DATA_STREAM_NAME + "fake*" + "/_stats");
            assertThat(getStatsResponse.getBody(), getStatsResponse.getStatusCode(), equalTo(200));
            assertThat(getStatsResponse.getBody(), getStatsResponse.getBodyAsDocNode().getAsListOfNodes("data_streams"), hasSize(0));
        }
    }

    @Test
    public void testRolloverDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            String dataStreamToRollover = FIRST_DATA_STREAM_NAME_PREFIX + "test-rollover";
            createDataStreams(dataStreamToRollover);

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            GenericRestClient.HttpResponse rolloverResponse = restClient.post("/" + dataStreamToRollover + "/_rollover");
            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            rolloverResponse = restClient.post("/" + FIRST_DATA_STREAM_NAME_PREFIX + "fake" + "/_rollover");
            //todo it returns 404, it should return 403?
//            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            rolloverResponse = restClient.post("/" + ADMIN_USER_DATA_STREAM_NAME + "/_rollover");
            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            rolloverResponse = restClient.post("/" + "not_existing" + "/_rollover");
            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String dataStreamToRollover = SECOND_DATA_STREAM_NAME_PREFIX + "test-rollover";
            createDataStreams(dataStreamToRollover);

            //user has access, data stream exists
            GenericRestClient.HttpResponse rolloverResponse = restClient.post("/" + dataStreamToRollover + "/_rollover");
            //todo it returns 404 no such index [.force_no_index*]
            // instead of 200
//            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(200));

            //user has access, data stream does not exist
            rolloverResponse = restClient.post("/" + SECOND_DATA_STREAM_NAME_PREFIX + "fake" + "/_rollover");
            //todo it returns 403 "missing_permissions":"data-stream-second-fake: indices:admin/rollover",
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            rolloverResponse = restClient.post("/" + ADMIN_USER_DATA_STREAM_NAME + "/_rollover/");
            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            rolloverResponse = restClient.post("/" + "not_existing" + "/_rollover/");
            assertThat(rolloverResponse.getBody(), rolloverResponse.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void testReindexDataStream() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {

            DocNode destRequest = DocNode.of("op_type", "create");
            DocNode sourceRequest = DocNode.of("index", SHARED_DATA_STREAM_NAME);

            //source exists, user has no access, perms are assigned to data stream not to backing indices
            destRequest = destRequest.with("index", FIRST_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            GenericRestClient.HttpResponse reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

            //source does not exist, user has no access, perms are assigned to data stream not to backing indices
            destRequest = destRequest.with("index", FIRST_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", SHARED_DATA_STREAM_NAME + "fake");
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            //todo it should return 403? it returns 404
//            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

            //source exists, user has no access, perms are assigned to data stream not to backing indices
            destRequest = destRequest.with("index", "fake-" + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", SHARED_DATA_STREAM_NAME);
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

            //source does not exist, user has no access, perms are assigned to data stream not to backing indices
            destRequest = destRequest.with("index", "fake-" + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", SHARED_DATA_STREAM_NAME + "fake");
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            //todo it should return 403? it returns 404
//            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

            //source exists, user has no access, perms are assigned to data stream not to backing indices
            destRequest = destRequest.with("index", FIRST_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", ADMIN_USER_DATA_STREAM_NAME);
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

            //source does not exist, user has no access, perms are assigned to data stream not to backing indices
            destRequest = destRequest.with("index", FIRST_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", ADMIN_USER_DATA_STREAM_NAME + "fake");
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            //todo it should return 403? it returns 404
//            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            DocNode destRequest = DocNode.of("op_type", "create");
            DocNode sourceRequest = DocNode.of("index", SHARED_DATA_STREAM_NAME);

            //user has access to source and dest, source exists
            destRequest = destRequest.with("index", SECOND_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            GenericRestClient.HttpResponse reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            //todo it returns 403 "missing_permissions":"data-stream-second-reindex-9e1febaf-3fc8-41cc-b8e7-de0de3d63981: indices:admin/auto_create"
            // but user has indices:admin/auto_create permission assigned to backing indices
//            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(200));

            //user has access to source and dest, source does not exist
            destRequest = destRequest.with("index", SECOND_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", SHARED_DATA_STREAM_NAME + "fake");
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(404));

            //user has access only to source, source exists
            destRequest = destRequest.with("index", "fake-" + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", SHARED_DATA_STREAM_NAME);
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));
            assertThat(reindexResponse.getBody(), reindexResponse.getBodyAsDocNode().getAsListOfNodes("failures").size(), greaterThan(0));

            //user has access only to source, source does not exist
            destRequest = destRequest.with("index", "fake-" + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", SHARED_DATA_STREAM_NAME + "fake");
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(404));

            //user has access only to dest, source exists
            destRequest = destRequest.with("index", SECOND_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", ADMIN_USER_DATA_STREAM_NAME);
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));

            //user has access only to dest, source does not exist
            destRequest = destRequest.with("index", SECOND_DATA_STREAM_NAME_PREFIX + "reindex-" + UUID.randomUUID());
            sourceRequest = sourceRequest.with("index", ADMIN_USER_DATA_STREAM_NAME + "fake");
            reindexResponse = restClient.postJson("/_reindex", DocNode.of("dest", destRequest, "source", sourceRequest));
            //todo it should return 403? it returns 404
//            assertThat(reindexResponse.getBody(), reindexResponse.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void testOpenClosedBackingIndices() throws Exception {

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_DATA_STREAMS)) {
            String dataStreamToOpenIndices = FIRST_DATA_STREAM_NAME_PREFIX + "open-closed-indices";
            createDataStreams(dataStreamToOpenIndices);

            //data stream exists, user has no access, perms are assigned to data stream not to backing indices
            GenericRestClient.HttpResponse openResponse = restClient.post("/" + dataStreamToOpenIndices + "/_open");
            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access, perms are assigned to data stream not to backing indices
            openResponse = restClient.post("/" + FIRST_DATA_STREAM_NAME_PREFIX + "fake" + "/_open");
            //todo it should return 403? it returns 404
//            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(403));

            //data stream exists, user has no access
            openResponse = restClient.post("/" + ADMIN_USER_DATA_STREAM_NAME + "/_open");
            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(403));

            //data stream does not exist, user has no access
            openResponse = restClient.post("/" + ADMIN_USER_DATA_STREAM_NAME + "fake" + "/_open");
            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(403));
        }

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {
            String dataStreamToOpenIndices = SECOND_DATA_STREAM_NAME_PREFIX + "open-closed-indices";
            createDataStreams(dataStreamToOpenIndices);

            //user has access, data stream exists
            GenericRestClient.HttpResponse openResponse = restClient.post("/" + dataStreamToOpenIndices + "/_open");
            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(200));

            //user has access, data stream does not exists
            openResponse = restClient.post("/" + SECOND_DATA_STREAM_NAME_PREFIX + "fake" + "/_open");
            //todo it returns 403 "missing_permissions":"data-stream-second-fake: indices:admin/open"
            // it should return 404, user has assigned permissions to backing indices
//            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(404));

            //user has no access, data stream exists
            openResponse = restClient.post("/" + ADMIN_USER_DATA_STREAM_NAME + "/_open");
            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(403));

            //user has no access, data stream does not exist
            openResponse = restClient.post("/" + ADMIN_USER_DATA_STREAM_NAME + "fake" + "/_open");
            assertThat(openResponse.getBody(), openResponse.getStatusCode(), equalTo(403));
        }
    }

    @Test
    public void testMigrateIndexAliasToDataStream() throws Exception {
        //todo "indices:admin/data_stream/migrate" permission needs to be assigned to index

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String indexToMigrate = INDEX_NAME_PREFIX + "-test-migrate-" + UUID.randomUUID();
            String aliasToMigrate = SECOND_DATA_STREAM_NAME_PREFIX + "test-index-alias-migrate-" + UUID.randomUUID();

            createIndices(indexToMigrate);
            createIndexAlias(indexToMigrate, aliasToMigrate);

            //alias exists, user has access to migrate write index
            GenericRestClient.HttpResponse migrateResponse = restClient.post("/_data_stream/_migrate/" + aliasToMigrate);
            assertThat(migrateResponse.getBody(), migrateResponse.getStatusCode(), equalTo(200));

            //alias does not exists
            migrateResponse = restClient.post("/_data_stream/_migrate/" + aliasToMigrate + "fake");
            assertThat(migrateResponse.getBody(), migrateResponse.getStatusCode(), equalTo(403));

            //alias exists, user has no access to migrate write index
            indexToMigrate = "index-test-migrate-" + UUID.randomUUID();
            aliasToMigrate = SECOND_DATA_STREAM_NAME_PREFIX + "test-index-alias-migrate-" + UUID.randomUUID();

            createIndices(indexToMigrate);
            createIndexAlias(indexToMigrate, aliasToMigrate);

            migrateResponse = restClient.post("/_data_stream/_migrate/" + aliasToMigrate);
            assertThat(migrateResponse.getBody(), migrateResponse.getStatusCode(), equalTo(403));

            //alias exists, user has access to migrate write index, but has no access to create data stream with given name (data stream name == alias name)
            indexToMigrate = INDEX_NAME_PREFIX + "-test-migrate-" + UUID.randomUUID();
            aliasToMigrate = FIRST_DATA_STREAM_NAME_PREFIX + "test-index-alias-migrate-" + UUID.randomUUID();

            createIndices(indexToMigrate);
            createIndexAlias(indexToMigrate, aliasToMigrate);

            migrateResponse = restClient.post("/_data_stream/_migrate/" + aliasToMigrate);
            //todo 200 or 403 expected?
            assertThat(migrateResponse.getBody(), migrateResponse.getStatusCode(), equalTo(200));
        }
    }

    @Test
    public void testModifyDataStream() throws Exception {
        //todo "indices:admin/data_stream/modify" permission needs to be assigned to index "indices:admin/data_stream/migrate"

        try (GenericRestClient restClient = CLUSTER.getRestClient(USER_WITH_PERMS_ASSIGNED_TO_BACKING_INDICES)) {

            String dataStreamWhichUserCanAccess = SECOND_DATA_STREAM_NAME_PREFIX + "test-modify";
            String indexWhichUserCanAccess = INDEX_NAME_PREFIX + UUID.randomUUID();
            String indexWhichUserCannotAccess = "index-which-user-cannot-access-" + UUID.randomUUID();

            createDataStreams(dataStreamWhichUserCanAccess);
            createIndices(indexWhichUserCanAccess, indexWhichUserCannotAccess);

            DocNode addBackingIndex = DocNode.of("data_stream", dataStreamWhichUserCanAccess, "index", indexWhichUserCanAccess);
            DocNode modifyRequestBody = DocNode.of("actions", ImmutableList.of(DocNode.of("add_backing_index", addBackingIndex)));

            //data stream & index exist, user has access to data stream & index
            GenericRestClient.HttpResponse modifyResponse = restClient.postJson("/_data_stream/_modify", modifyRequestBody);
            assertThat(modifyResponse.getBody(), modifyResponse.getStatusCode(), equalTo(200));

            //only data stream exists, user has access to data stream & index
            indexWhichUserCanAccess = INDEX_NAME_PREFIX + "fake-" + UUID.randomUUID();
            addBackingIndex = DocNode.of("data_stream", dataStreamWhichUserCanAccess, "index", indexWhichUserCanAccess);
            modifyRequestBody = DocNode.of("actions", ImmutableList.of(DocNode.of("add_backing_index", addBackingIndex)));
            modifyResponse = restClient.postJson("/_data_stream/_modify", modifyRequestBody);
            assertThat(modifyResponse.getBody(), modifyResponse.getStatusCode(), equalTo(400));

            //only index exists, user has access to data stream & index
            indexWhichUserCanAccess = INDEX_NAME_PREFIX + UUID.randomUUID();
            createIndices(indexWhichUserCanAccess);
            addBackingIndex = DocNode.of("data_stream", dataStreamWhichUserCanAccess + "-fake", "index", indexWhichUserCanAccess);
            modifyRequestBody = DocNode.of("actions", ImmutableList.of(DocNode.of("add_backing_index", addBackingIndex)));
            modifyResponse = restClient.postJson("/_data_stream/_modify", modifyRequestBody);
            assertThat(modifyResponse.getBody(), modifyResponse.getStatusCode(), equalTo(400));

            //data stream & index exist, user has access only to data stream
            addBackingIndex = DocNode.of("data_stream", dataStreamWhichUserCanAccess, "index", indexWhichUserCannotAccess);
            modifyRequestBody = DocNode.of("actions", ImmutableList.of(DocNode.of("add_backing_index", addBackingIndex)));

            modifyResponse = restClient.postJson("/_data_stream/_modify", modifyRequestBody);
            assertThat(modifyResponse.getBody(), modifyResponse.getStatusCode(), equalTo(403));

            //data stream & index exist, user has access only to index
            indexWhichUserCanAccess = INDEX_NAME_PREFIX + UUID.randomUUID();
            createIndices(indexWhichUserCanAccess);
            addBackingIndex = DocNode.of("data_stream", ADMIN_USER_DATA_STREAM_NAME, "index", indexWhichUserCanAccess);
            modifyRequestBody = DocNode.of("actions", ImmutableList.of(DocNode.of("add_backing_index", addBackingIndex)));

            modifyResponse = restClient.postJson("/_data_stream/_modify", modifyRequestBody);
            //todo it returns 200 but user has no access to data stream
//            assertThat(modifyResponse.getBody(), modifyResponse.getStatusCode(), equalTo(403));
        }
    }

    private static void createDataStreams(String... dataStreamNames) throws Exception {
        try (GenericRestClient restClient = CLUSTER.getRestClient(ADMIN_USER)) {
            for (String name: dataStreamNames) {
                GenericRestClient.HttpResponse response = restClient.put("/_data_stream/" + name);
                assertThat(response.getBody(), response.getStatusCode(), equalTo(200));
            }
        }
    }

    private static void createDocument(String dataStream, String id, DocNode doc) {
        try (Client client = CLUSTER.getInternalNodeClient()) {
            IndexRequest indexRequest = new org.elasticsearch.action.index.IndexRequest(dataStream)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .source(doc)
                    .id(id)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
           IndexResponse indexResponse = client.index(indexRequest).actionGet();
           assertThat(indexResponse.status().getStatus(), equalTo(201));
        }
    }

    private static void createIndices(String... indexNames) {
        try (Client client = CLUSTER.getInternalNodeClient()) {
            for (String name: indexNames) {
                CreateIndexResponse createIndexResponse = client.admin().indices()
                        .create(new CreateIndexRequest(name).mapping("_doc","@timestamp","type=date")).actionGet();
                assertThat(createIndexResponse.isAcknowledged(), is(true));
            }
        }
    }

    private static void createIndexAlias(String indexName, String aliasName) {
        try (Client client = CLUSTER.getInternalNodeClient()) {
            AcknowledgedResponse aliasResponse = client.admin().indices().aliases(
                    new IndicesAliasesRequest()
                            .addAliasAction(IndicesAliasesRequest.AliasActions.add().indices(indexName).alias(aliasName).writeIndex(true))
            ).actionGet();
            assertThat(aliasResponse.isAcknowledged(), is(true));
        }
    }
}

