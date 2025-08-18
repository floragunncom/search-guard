package com.floragunn.searchguard.enterprise.auditlog.impl;

import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.CATEGORY;
import static com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.FORMAT_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.auditlog.AuditLog;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.floragunn.searchsupport.util.EsLogging;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.index.IndexVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuditTestToXContentObjectImplTest {
    static {
        EsLogging.initLogging();
    }

    @Mock private ClusterState clusterState;
    @Mock private DiscoveryNode localNode;
    private final String clusterName = "test-cluster";
    private final String localNodeId = "test-local-node-id";
    private final String localNodeName = "test-local-node";
    private final String localNodeHostAddress = "127.0.0.1";
    private final String localNodeHostName = "test-local-node-host";
    private final Version localNodeVersion = Version.CURRENT;

    @Before
    public void setUp() {
        when(clusterState.getClusterName()).thenReturn(new ClusterName(clusterName));
        when(localNode.getId()).thenReturn(localNodeId);
        when(localNode.getName()).thenReturn(localNodeName);
        when(localNode.getHostAddress()).thenReturn(localNodeHostAddress);
        when(localNode.getHostName()).thenReturn(localNodeHostName);
        when(localNode.getVersion()).thenReturn(localNodeVersion);
        when(localNode.getMinIndexVersion()).thenReturn(IndexVersion.current());
        when(localNode.getMinReadOnlyIndexVersion()).thenReturn(IndexVersion.current());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(localNode).localNodeId(localNodeId).build();
        when(clusterState.nodes()).thenReturn(discoveryNodes);
    }

    @Test
    public void initialize_commonAttributesShouldBeSet_allConstructorParamsAreProvided() {
        AuditMessage.Category category = AuditMessage.Category.FAILED_LOGIN;
        AuditLog.Origin origin = AuditLog.Origin.REST;
        AuditLog.Origin requestLayer = AuditLog.Origin.TRANSPORT;

        AuditMessage auditMessage = new AuditMessage(category, clusterState, origin, requestLayer);

        assertThat(auditMessage.getAsMap().get(AuditMessage.FORMAT_VERSION), notNullValue());
        assertThat(auditMessage.getAsMap().get(AuditMessage.CATEGORY), equalTo(category));
        assertThat(auditMessage.getAsMap().get(AuditMessage.UTC_TIMESTAMP), notNullValue());
        assertThat(auditMessage.getAsMap().get(AuditMessage.CLUSTER_NAME), equalTo(clusterName));
        assertThat(auditMessage.getAsMap().get(AuditMessage.NODE_HOST_ADDRESS), equalTo(localNodeHostAddress));
        assertThat(auditMessage.getAsMap().get(AuditMessage.NODE_ID), equalTo(localNodeId));
        assertThat(auditMessage.getAsMap().get(AuditMessage.NODE_HOST_NAME), equalTo(localNodeHostName));
        assertThat(auditMessage.getAsMap().get(AuditMessage.NODE_NAME), equalTo(localNodeName));
        assertThat(auditMessage.getAsMap().get(AuditMessage.NODE_VERSION), equalTo(localNodeVersion));
        assertThat(auditMessage.getAsMap().get(AuditMessage.ORIGIN), equalTo(origin));
        assertThat(auditMessage.getAsMap().get(AuditMessage.REQUEST_LAYER), equalTo(requestLayer));
    }

    @Test(expected = NullPointerException.class)
    public void initialize_shouldThrowException_categoryIsNull() {
        AuditLog.Origin origin = AuditLog.Origin.REST;
        AuditLog.Origin requestLayer = AuditLog.Origin.TRANSPORT;

        new AuditMessage(null, clusterState, origin, requestLayer);
    }

    @Test
    public void initialize_commonAttributesShouldBeSet_allParamsExceptCategoryAreNull() {
        AuditMessage.Category category = AuditMessage.Category.FAILED_LOGIN;
        AuditLog.Origin origin = AuditLog.Origin.REST;
        AuditLog.Origin requestLayer = AuditLog.Origin.LOCAL;

        AuditMessage auditMessage = new AuditMessage(category, null, origin, requestLayer);

        assertThat(auditMessage.getAsMap().get(AuditMessage.FORMAT_VERSION), notNullValue());
        assertThat(auditMessage.getAsMap().get(AuditMessage.CATEGORY), equalTo(category));
        assertThat(auditMessage.getAsMap().get(AuditMessage.UTC_TIMESTAMP), notNullValue());
        assertThat(auditMessage.getAsMap(), not(hasKey(AuditMessage.CLUSTER_NAME)));
        assertThat(auditMessage.getAsMap(), not(hasKey(AuditMessage.NODE_HOST_ADDRESS)));
        assertThat(auditMessage.getAsMap(), not(hasKey(AuditMessage.NODE_ID)));
        assertThat(auditMessage.getAsMap(), not(hasKey(AuditMessage.NODE_HOST_NAME)));
        assertThat(auditMessage.getAsMap(), not(hasKey(AuditMessage.NODE_NAME)));
        assertThat(auditMessage.getAsMap(), not(hasKey(AuditMessage.NODE_VERSION)));
        assertThat(auditMessage.getAsMap().get(AuditMessage.ORIGIN), equalTo(origin));
        assertThat(auditMessage.getAsMap().get(AuditMessage.REQUEST_LAYER), equalTo(requestLayer));
    }

    @Test
    public void auditMessageShouldNotContainDisabledFields() throws DocumentParseException {
        List<String> disabledFields = Arrays.asList(FORMAT_VERSION, CATEGORY);
        AuditMessage.Category category = AuditMessage.Category.FAILED_LOGIN;
        AuditLog.Origin origin = AuditLog.Origin.REST;
        AuditLog.Origin requestLayer = AuditLog.Origin.TRANSPORT;

        //before disabling
        AuditMessage am = new AuditMessage(category, clusterState, origin, requestLayer);
        Map<String, Object> asMap = am.getAsMap();
        String toString = am.toString();
        String toPrettyString = am.toPrettyString();
        String toJson = am.toJson();
        String toText = am.toText();
        String toUrlParameters = am.toUrlParameters().replace("?", "");
        List<NameValuePair> queryParams = URLEncodedUtils.parse(toUrlParameters, StandardCharsets.UTF_8);

        assertThat(asMap.keySet(), hasItems(disabledFields.toArray(new String[] {})));
        assertThat(DocNode.parse(Format.JSON).from(toString).keySet(), hasItems(disabledFields.toArray(new String[] {})));
        assertThat(DocNode.parse(Format.JSON).from(toPrettyString).keySet(), hasItems(disabledFields.toArray(new String[] {})));
        assertThat(DocNode.parse(Format.JSON).from(toJson).keySet(), hasItems(disabledFields.toArray(new String[] {})));
        assertThat(
                disabledFields.stream().allMatch(field -> queryParams.stream().anyMatch(param -> param.getName().equals(field))),
                is(true)
        );
        assertThat(disabledFields.stream().allMatch(field -> toText.contains(field + ":")), is(true));

        //after disabling
        am.removeDisabledFields(disabledFields);
        Map<String, Object> asMapWithoutDisabled = am.getAsMap();
        String toStringWithoutDisabled = am.toString();
        String toPrettyStringWithoutDisabled = am.toPrettyString();
        String toJsonWithoutDisabled = am.toJson();
        String toTextWithoutDisabled = am.toText();
        String toUrlParametersWithoutDisabled = am.toUrlParameters().replace("?", "");
        List<NameValuePair> queryParamsWithoutDisabled = URLEncodedUtils.parse(toUrlParametersWithoutDisabled, StandardCharsets.UTF_8);

        assertThat(asMapWithoutDisabled.keySet(), not(hasItems(disabledFields.toArray(new String[] {}))));
        assertThat(DocNode.parse(Format.JSON).from(toStringWithoutDisabled).keySet(), not(hasItems(disabledFields.toArray(new String[] {}))));
        assertThat(DocNode.parse(Format.JSON).from(toPrettyStringWithoutDisabled).keySet(), not(hasItems(disabledFields.toArray(new String[] {}))));
        assertThat(DocNode.parse(Format.JSON).from(toJsonWithoutDisabled).keySet(), not(hasItems(disabledFields.toArray(new String[] {}))));
        assertThat(
                disabledFields.stream().noneMatch(field -> queryParamsWithoutDisabled.stream().anyMatch(param -> param.getName().equals(field))),
                is(true)
        );
        assertThat(disabledFields.stream().noneMatch(field -> toTextWithoutDisabled.contains(field + ":")), is(true));

    }
}
