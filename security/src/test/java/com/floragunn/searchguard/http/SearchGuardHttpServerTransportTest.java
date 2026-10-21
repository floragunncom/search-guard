/*
 * Copyright 2024-2025 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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