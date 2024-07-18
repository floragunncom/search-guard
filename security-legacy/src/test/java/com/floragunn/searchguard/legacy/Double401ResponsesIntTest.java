package com.floragunn.searchguard.legacy;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.authc.rest.AuthenticatingRestFilter;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServerTransport.Dispatcher;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Double401ResponsesIntTest {

    private static final Logger log = LogManager.getLogger(Double401ResponsesIntTest.class);

    private static TestSgConfig.User USER = new TestSgConfig.User("user") //
        .roles(new TestSgConfig.Role("role").indexPermissions("*").on("*"));

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder()
            .singleNode()
            .sslEnabled()
            .resources("doubleUnauthorized")
            .users(USER)
            .embedded()
            .build();

    @Mock
    private RestRequest restRequest;
    @Mock
    private RestChannel restChannel;
    @Mock
    private HttpChannel httpChannel;
    @Captor
    private ArgumentCaptor<RestResponse> responseCaptor;
    @Mock
    private Dispatcher genuineDispatcher;
    @Mock
    private ThreadContext threadContext;
    @Mock
    private HttpRequest httpRequest;
    private Dispatcher searchGuarddispatcher;

    // under test
    private AuthenticatingRestFilter authenticatingRestFilter;


    @Before
    public void before() {
        this.authenticatingRestFilter = cluster.getInjectable(AuthenticatingRestFilter.class);
        this.searchGuarddispatcher = authenticatingRestFilter.wrap(genuineDispatcher);
    }


    @Test
    @Ignore("This test is for manual execution only, to reproduce bug related to double 401 responses")
    public void reproduceDouble401ResponsesBug() throws Exception {
        try (GenericRestClient client = cluster.getRestClient(new BasicHeader("Authorization", "Bearer invalid_token"))) {
            GenericRestClient.HttpResponse response = client.get("_searchguard/authinfo");
            log.debug("Actual response is '{}'", response.getBody());
        }
    }

    @Test
    public void shouldSend401ResponseOnce() {
        when(restRequest.getHttpRequest()).thenReturn(httpRequest);
        when(restChannel.request()).thenReturn(restRequest);
        when(restRequest.getHttpChannel()).thenReturn(httpChannel);
        when(restRequest.path()).thenReturn("/_searchguard/authinfo");
        when(restRequest.method()).thenReturn(GET);
        when(restRequest.getHeaders()).thenReturn(ImmutableMap.of("Authorization", ImmutableList.of("Bearer invalid_token")));
        when(restRequest.getRequestId()).thenReturn(-1234L);
        when((restRequest.getSpanId())).thenReturn("test span id");
        when(httpChannel.getRemoteAddress()).thenReturn(new InetSocketAddress("localhost", 0));

        searchGuarddispatcher.dispatchRequest(restRequest, restChannel, threadContext);

        verify(restChannel, times(1)).sendResponse(responseCaptor.capture());
        RestResponse response = responseCaptor.getValue();
        assertThat(response.status(), is(UNAUTHORIZED));
        assertThat(response.getHeaders(), hasEntry(equalTo("WWW-Authenticate"), contains("Basic realm=\"Search Guard\"")));
        verifyNoInteractions(genuineDispatcher, threadContext);
    }
}
