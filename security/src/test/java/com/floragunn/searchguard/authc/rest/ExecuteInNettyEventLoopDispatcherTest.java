/*
 * Copyright 2025 floragunn GmbH
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
package com.floragunn.searchguard.authc.rest;

import com.floragunn.searchsupport.rest.AttributedHttpRequest;
import com.floragunn.searchsupport.util.EsLogging;
import io.netty.channel.EventLoop;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpServerTransport.Dispatcher;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExecuteInNettyEventLoopDispatcherTest {

    @ClassRule
    public static EsLogging esLogging = new EsLogging();

    @Mock
    private Dispatcher originalDispatcher;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private ThreadContext threadContext;
    @Mock
    private RestRequest restRequest;
    @Mock
    private RestChannel restChannel;
    @Mock
    private HttpRequest httpRequest;
    @Mock
    private AttributedHttpRequest attributedHttpRequest;
    @Mock
    private EventLoop eventLoop;
    @Mock
    private Runnable preservedRunnable;
    @Mock
    private Throwable cause;

    private ExecuteInNettyEventLoopDispatcher dispatcher;

    @Before
    public void setUp() {
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        dispatcher = new ExecuteInNettyEventLoopDispatcher(originalDispatcher, threadPool);
    }

    @Test
    public void shouldDelegateDispatchRequestToOriginalDispatcherWhenInEventLoop() {
        when(restRequest.getHttpRequest()).thenReturn(attributedHttpRequest);
        when(attributedHttpRequest.getAttribute("sg_event_loop")).thenReturn(Optional.of(eventLoop));
        when(eventLoop.inEventLoop()).thenReturn(true);

        dispatcher.dispatchRequest(restRequest, restChannel, threadContext);

        verify(originalDispatcher).dispatchRequest(restRequest, restChannel, threadContext);
    }

    @Test
    public void shouldPreserveContextAndExecuteOnEventLoopWhenNotInEventLoop() {
        when(restRequest.getHttpRequest()).thenReturn(attributedHttpRequest);
        when(attributedHttpRequest.getAttribute("sg_event_loop")).thenReturn(Optional.of(eventLoop));
        when(eventLoop.inEventLoop()).thenReturn(false);
        doReturn(preservedRunnable).when(threadContext).preserveContext(Mockito.any(Runnable.class));

        dispatcher.dispatchRequest(restRequest, restChannel, threadContext);

        verify(eventLoop).execute(preservedRunnable);
    }

    @Test
    public void shouldLogErrorWhenEventLoopIsNull() {
        // AttributedHttpRequest, eventLoop attribute missing
        when(restRequest.getHttpRequest()).thenReturn(attributedHttpRequest);
        when(attributedHttpRequest.getAttribute("sg_event_loop")).thenReturn(Optional.empty());

        Throwable throwable = assertThatThrown(() -> dispatcher.dispatchRequest(restRequest, restChannel, threadContext),
                instanceOf(AssertionError.class));

        assertThat(throwable.getMessage(), containsString("Netty event loop not present, cannot use correct thread"));
    }

    @Test
    public void shouldLogErrorWhenHttpRequestIsNotAttributed() {
        when(restRequest.getHttpRequest()).thenReturn(httpRequest);

        Throwable throwable = assertThatThrown(() -> dispatcher.dispatchRequest(restRequest, restChannel, threadContext),
                instanceOf(AssertionError.class));

        assertThat(throwable.getMessage(), containsString("Expected AttributedHttpRequest but got"));
    }

    @Test
    public void shouldDelegateDispatchBadRequestToOriginalDispatcherWhenInEventLoop() {
        when(restChannel.request()).thenReturn(restRequest);
        when(restRequest.getHttpRequest()).thenReturn(attributedHttpRequest);
        when(attributedHttpRequest.getAttribute("sg_event_loop")).thenReturn(Optional.of(eventLoop));
        when(eventLoop.inEventLoop()).thenReturn(true);

        dispatcher.dispatchBadRequest(restChannel, threadContext, cause);

        verify(originalDispatcher).dispatchBadRequest(restChannel, threadContext, cause);
    }

    @Test
    public void shouldPreserveContextAndExecuteBadRequestOnEventLoopWhenNotInEventLoop() {
        when(restChannel.request()).thenReturn(restRequest);
        when(restRequest.getHttpRequest()).thenReturn(attributedHttpRequest);
        when(attributedHttpRequest.getAttribute("sg_event_loop")).thenReturn(Optional.of(eventLoop));
        when(eventLoop.inEventLoop()).thenReturn(false);
        doReturn(preservedRunnable).when(threadContext).preserveContext(Mockito.any(Runnable.class));

        dispatcher.dispatchBadRequest(restChannel, threadContext, cause);

        verify(eventLoop).execute(preservedRunnable);
    }

    @Test
    public void shouldLogErrorWhenEventLoopIsNullForDispatchBadRequest() {
        when(restChannel.request()).thenReturn(restRequest);
        when(restRequest.getHttpRequest()).thenReturn(attributedHttpRequest);
        when(attributedHttpRequest.getAttribute("sg_event_loop")).thenReturn(Optional.empty());

        Throwable throwable = assertThatThrown(() -> dispatcher.dispatchBadRequest(restChannel, threadContext, cause),
                instanceOf(AssertionError.class));

        assertThat(throwable.getMessage(), containsString("Netty event loop not present, cannot use correct thread"));
    }

    @Test
    public void shouldLogErrorWhenHttpRequestIsNotAttributedForDispatchBadRequest() {
        when(restChannel.request()).thenReturn(restRequest);
        when(restRequest.getHttpRequest()).thenReturn(httpRequest);

        Throwable throwable = assertThatThrown(() -> dispatcher.dispatchBadRequest(restChannel, threadContext, cause),
                instanceOf(AssertionError.class));

        assertThat(throwable.getMessage(), containsString("Expected AttributedHttpRequest but got"));
    }
}
