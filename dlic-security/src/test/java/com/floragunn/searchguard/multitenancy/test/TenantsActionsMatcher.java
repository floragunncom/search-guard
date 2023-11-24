package com.floragunn.searchguard.multitenancy.test;

import static com.floragunn.searchguard.multitenancy.test.TenantAccessMatcher.Action.CREATE_DOCUMENT;
import static com.floragunn.searchguard.multitenancy.test.TenantAccessMatcher.Action.DELETE_INDEX;
import static com.floragunn.searchguard.multitenancy.test.TenantAccessMatcher.Action.UPDATE_DOCUMENT;
import static com.floragunn.searchguard.multitenancy.test.TenantAccessMatcher.Action.UPDATE_INDEX;

import com.floragunn.searchguard.multitenancy.test.TenantAccessMatcher.Action;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient;
import com.floragunn.searchguard.test.helper.rest.GenericRestClient.HttpResponse;
import java.util.EnumSet;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class TenantsActionsMatcher extends TypeSafeDiagnosingMatcher<GenericRestClient> {

    private final EnumSet<Action> allowedActions;

    public TenantsActionsMatcher(EnumSet<Action> allowedActions) {
        this.allowedActions = allowedActions;
    }

    @Override protected boolean matchesSafely(GenericRestClient restClient, Description mismatchDescription) {
        try {
            final HttpResponse search = restClient.get(".kibana/_search");
            if (search.getStatusCode() != HttpStatus.SC_OK ){
                mismatchDescription.appendText(".kibana/_search GET call response is not HTTP/200");
                return false;
            }

            final HttpResponse msearch = restClient.postJson(".kibana/_msearch", "{}\n{\"query\":{\"match_all\":{}}}\n");
            if (msearch.getStatusCode() != HttpStatus.SC_OK) {
                mismatchDescription.appendText(".kibana/_msearch POST call response is not HTTP/200");
                return false;
            }

            final HttpResponse mget = restClient.postJson(".kibana/_mget", "{\"docs\":[{\"_id\":\"5.6.0\"}]}");
            if (mget.getStatusCode() != HttpStatus.SC_OK) {
                mismatchDescription.appendText(".kibana/_mget POST call response is not HTTP/200");
                return false;
            }

            final HttpResponse getDoc = restClient.get(".kibana/_doc/5.6.0");
            if (getDoc.getStatusCode() != HttpStatus.SC_OK) {
                mismatchDescription.appendText(".kibana/_doc/5.6.0 GET call response is not HTTP/200");
                return false;
            }

            final HttpResponse createDoc = restClient.postJson(".kibana/_doc", "{}");
            int expectedStatus = allowedActions.contains(CREATE_DOCUMENT) ? HttpStatus.SC_CREATED : HttpStatus.SC_FORBIDDEN;
            if (createDoc.getStatusCode() != expectedStatus) {
                mismatchDescription.appendText(".kibana/_doc POST call response is not HTTP/").appendValue(expectedStatus);
                return false;
            }

            final HttpResponse updateDoc = restClient.putJson(".kibana/_doc/5.6.0", "{}");
            expectedStatus = allowedActions.contains(UPDATE_DOCUMENT) ? HttpStatus.SC_OK : HttpStatus.SC_FORBIDDEN ;
            if (updateDoc.getStatusCode() != expectedStatus) {
                mismatchDescription.appendText(".kibana/_doc/5.6.0 PUT call response is not HTTP/").appendValue(expectedStatus);
                return false;
            }

            final HttpResponse deleteDoc = restClient.delete(".kibana/_doc/5.6.0");
            expectedStatus = allowedActions.contains(UPDATE_DOCUMENT) ? HttpStatus.SC_OK : HttpStatus.SC_FORBIDDEN ;
            if (deleteDoc.getStatusCode() != expectedStatus) {
                mismatchDescription.appendText(".kibana/_doc/5.6.0 DELETE call response is not HTTP/").appendValue(expectedStatus);
                return false;
            }

            final HttpResponse getKibana = restClient.get(".kibana");
            if (getKibana.getStatusCode() != HttpStatus.SC_OK) {
                mismatchDescription.appendText(".kibana GET call response is not HTTP/200");
                return false;
            }

            final HttpResponse closeKibana = restClient.post(".kibana/_close");
            expectedStatus = allowedActions.contains(UPDATE_INDEX) ? HttpStatus.SC_OK : HttpStatus.SC_FORBIDDEN  ;
            if (closeKibana.getStatusCode() != expectedStatus) {
                mismatchDescription.appendText(".kibana/_close POST call response is not HTTP/").appendValue(expectedStatus);
                return false;
            }

            final HttpResponse deleteKibana = restClient.delete(".kibana");
            expectedStatus = allowedActions.contains(DELETE_INDEX) ? HttpStatus.SC_OK : HttpStatus.SC_FORBIDDEN;
            if (deleteKibana.getStatusCode() != expectedStatus) {
                mismatchDescription.appendText(".kibana delete call response is not HTTP/").appendValue(expectedStatus);
                return false;
            }
        } catch (Exception e){
            return false;
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("User should be allowed to perform the following actions" +
            allowedActions.stream()
                .map(Enum::toString)
                .collect(Collectors.joining(", ")));
    }
}