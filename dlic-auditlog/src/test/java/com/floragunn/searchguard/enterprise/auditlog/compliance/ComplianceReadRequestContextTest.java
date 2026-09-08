/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.auditlog.compliance;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.enterprise.auditlog.AbstractAuditlogiUnitTest;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.enterprise.auditlog.integration.TestAuditlogImpl;
import com.floragunn.searchguard.legacy.test.DynamicSgConfig;
import com.floragunn.searchguard.legacy.test.RestHelper.HttpResponse;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.helper.cluster.ClusterConfiguration;
import com.floragunn.searchsupport.junit.AsyncAssert;

/**
 * A compliance read event has to describe where the read came from: which user, from which address, over which
 * channel. The user is covered elsewhere; this test covers the other two.
 *
 * Both are carried in thread context transients (SG_ORIGIN, SG_REMOTE_ADDRESS) whose header counterparts
 * (SG_ORIGIN_HEADER, SG_REMOTE_ADDRESS_HEADER) are only created inside the stashed context which
 * SearchGuardInterceptor#sendRequestDecorate opens per outgoing request. They are therefore absent from the thread
 * context of the coordinating node itself, which is what Elasticsearch snapshots in
 * SearchTransportService#sendExecuteFetch before TransportFetchPhaseCoordinationAction stashes the context. On the
 * chunked fetch phase they consequently do not reach the data node:
 *
 * - the origin arrives as LOCAL, because SearchGuardInterceptor#ensureCorrectHeaders manufactures Origin.LOCAL when
 *   the origin transient is gone,
 * - the remote address does not arrive at all.
 *
 * Each assertion is therefore made three times: with the chunked fetch phase switched off, which is the classic code
 * path and the control for this test, with it switched on, and with whatever Elasticsearch currently defaults to. The
 * last one is what users actually get; the first two make visible which of the two paths a failure belongs to.
 */
public class ComplianceReadRequestContextTest extends AbstractAuditlogiUnitTest {

    @Test
    public void originOfComplianceDocReadIsRest_classicFetchPhase() throws Exception {
        AuditMessage message = complianceDocReadOfSearch(Boolean.FALSE);

        Assert.assertEquals(message.toPrettyString(), "REST", String.valueOf(message.getAsMap().get(AuditMessage.ORIGIN)));
    }

    @Test
    public void originOfComplianceDocReadIsRest_esDefaults() throws Exception {
        AuditMessage message = complianceDocReadOfSearch(null);

        Assert.assertEquals(message.toPrettyString(), "REST", String.valueOf(message.getAsMap().get(AuditMessage.ORIGIN)));
    }

    @Test
    public void originOfComplianceDocReadIsRest_chunkedFetchPhase() throws Exception {
        AuditMessage message = complianceDocReadOfSearch(Boolean.TRUE);

        Assert.assertEquals(message.toPrettyString(), "REST", String.valueOf(message.getAsMap().get(AuditMessage.ORIGIN)));
    }

    @Test
    public void remoteAddressOfComplianceDocReadIsPresent_classicFetchPhase() throws Exception {
        AuditMessage message = complianceDocReadOfSearch(Boolean.FALSE);

        Assert.assertNotNull(message.toPrettyString(), message.getAsMap().get(AuditMessage.REMOTE_ADDRESS));
    }

    @Test
    public void remoteAddressOfComplianceDocReadIsPresent_esDefaults() throws Exception {
        AuditMessage message = complianceDocReadOfSearch(null);

        Assert.assertNotNull(message.toPrettyString(), message.getAsMap().get(AuditMessage.REMOTE_ADDRESS));
    }

    @Test
    public void remoteAddressOfComplianceDocReadIsPresent_chunkedFetchPhase() throws Exception {
        AuditMessage message = complianceDocReadOfSearch(Boolean.TRUE);

        Assert.assertNotNull(message.toPrettyString(), message.getAsMap().get(AuditMessage.REMOTE_ADDRESS));
    }

    /**
     * Runs a search which reads a watched field and returns the resulting COMPLIANCE_DOC_READ message.
     *
     * @param chunkedFetchPhase value for search.fetch_phase_chunked_enabled, null to use the Elasticsearch default
     */
    private AuditMessage complianceDocReadOfSearch(Boolean chunkedFetchPhase) throws Exception {
        Settings.Builder additionalSettings = Settings.builder()
                .put("searchguard.audit.type", TestAuditlogImpl.class.getName())
                .put(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENABLED, false)
                .put(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_WATCHED_FIELDS, "emp")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, "authenticated,GRANTED_PRIVILEGES")
                .put("searchguard.audit.threadpool.size", 0);

        if (chunkedFetchPhase != null) {
            additionalSettings.put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), chunkedFetchPhase.booleanValue());
        }

        // A single node keeps the assertion about the remote address deterministic. The fetch is then served over a
        // direct channel by the very node which received the REST request, so a lost SG_REMOTE_ADDRESS shows up as a
        // missing field. On a multi node localhost cluster SearchGuardRequestHandler falls back to the address of the
        // sending node, which is 127.0.0.1 as well and therefore indistinguishable from the address of the client.
        setup(Settings.EMPTY, new DynamicSgConfig(), defaultNodeSettings(additionalSettings.build()), true,
                ClusterConfiguration.SINGLENODE);
        rh = restHelper();

        final boolean sendHTTPClientCertificate = rh.sendHTTPClientCertificate;
        final String keystore = rh.keystore;
        rh.sendHTTPClientCertificate = true;
        rh.keystore = "auditlog/kirk-keystore.jks";
        rh.executePutRequest("emp/_doc/0?refresh", "{\"Designation\" : \"CEO\", \"Gender\" : \"female\", \"Salary\" : 100}", new Header[0]);
        rh.executePutRequest("emp/_doc/1?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"male\", \"Salary\" : 200}", new Header[0]);
        rh.executePutRequest("emp/_doc/2?refresh", "{\"Designation\" : \"IT\", \"Gender\" : \"female\", \"Salary\" : 300}", new Header[0]);
        rh.sendHTTPClientCertificate = sendHTTPClientCertificate;
        rh.keystore = keystore;

        String search = "{" //
                + "   \"_source\":[ \"Gender\" ]," //
                + "   \"from\":0," //
                + "   \"size\":3," //
                + "   \"query\":{ \"term\":{ \"Salary\": 300 } }" //
                + "}";

        TestAuditlogImpl.clear();

        HttpResponse response = rh.executePostRequest("_search?pretty", search, encodeBasicHeader("admin", "admin"));
        Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());

        AsyncAssert.awaitAssert("A COMPLIANCE_DOC_READ message arrived", () -> complianceDocRead() != null, Duration.ofSeconds(10));

        return complianceDocRead();
    }

    private AuditMessage complianceDocRead() {
        for (AuditMessage message : snapshotOfMessages()) {
            if (message.getCategory() == AuditMessage.Category.COMPLIANCE_DOC_READ) {
                return message;
            }
        }

        return null;
    }

    private static List<AuditMessage> snapshotOfMessages() {
        // The sink is configured with searchguard.audit.threadpool.size 0, so it stores on the request thread and the
        // messages of the search are complete once its response has been received.
        return new ArrayList<>(TestAuditlogImpl.messages);
    }
}
