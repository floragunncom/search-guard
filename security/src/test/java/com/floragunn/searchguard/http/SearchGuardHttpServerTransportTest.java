package com.floragunn.searchguard.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SearchGuardHttpServerTransportTest {

    @Mock
    private HttpRequest request;

    @Mock
    private HttpBody.Stream streamBody;

    @Mock
    private HttpBody.Full fullBody;

    @Mock
    private ReleasableBytesReference bytesReference;

    @Test
    public void shouldHaveBody() {
        when(streamBody.isFull()).thenReturn(false);
        when(request.body()).thenReturn(streamBody);


        boolean empty = SearchGuardHttpServerTransport.hasEmptyBody(request);

        assertThat(empty, is(false));
    }

    @Test
    public void shouldHaveFullEmptyBodyWhenByteReferenceIsZero() {
        when(fullBody.isFull()).thenReturn(true);
        when(fullBody.bytes()).thenReturn(bytesReference);
        when(fullBody.asFull()).thenReturn(fullBody);
        when(request.body()).thenReturn(fullBody);
        when(bytesReference.length()).thenReturn(0);

        boolean empty = SearchGuardHttpServerTransport.hasEmptyBody(request);

        assertThat(empty, is(true));
    }

    @Test
    public void shouldHaveFullEmptyBodyWhenByteReferenceIsNull() {
        when(fullBody.isFull()).thenReturn(true);
        when(fullBody.bytes()).thenReturn(null);
        when(fullBody.asFull()).thenReturn(fullBody);
        when(request.body()).thenReturn(fullBody);

        boolean empty = SearchGuardHttpServerTransport.hasEmptyBody(request);

        assertThat(empty, is(true));
    }

    @Test
    public void shouldHaveNonEmptyFullBody() {
        when(fullBody.isFull()).thenReturn(true);
        when(fullBody.bytes()).thenReturn(bytesReference);
        when(fullBody.asFull()).thenReturn(fullBody);
        when(request.body()).thenReturn(fullBody);
        when(bytesReference.length()).thenReturn(1);

        boolean empty = SearchGuardHttpServerTransport.hasEmptyBody(request);

        assertThat(empty, is(false));
    }
}