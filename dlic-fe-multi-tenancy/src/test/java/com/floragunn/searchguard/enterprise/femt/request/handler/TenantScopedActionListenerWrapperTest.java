/*
 * Copyright 2023-2024 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.request.handler;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TenantScopedActionListenerWrapperTest {

    @Mock
    private NodeClient client;

    @Test
    public void shouldCallDelegateOnFailure_whenExecutionFails() {

        RuntimeException actionException = new RuntimeException("Action failed");

        ActionListener<?> originalListener = Mockito.spy(ActionListener.noop());
        TenantScopedActionListenerWrapper<BulkByScrollResponse> listenerWrapper = new TenantScopedActionListenerWrapper<>(
                originalListener, (response) -> {}, (response) -> response, (ex) -> {}
        );

        doAnswer(new CallListenerOnFailureAnswer(actionException)).when(client).execute(any(), any(), any(ActionListener.class));

        client.execute(UpdateByQueryAction.INSTANCE, new UpdateByQueryRequest(), listenerWrapper);

        verify(originalListener).onFailure(eq(actionException));
    }

    private static class CallListenerOnFailureAnswer implements Answer<Void> {

        private final Exception ex;

        CallListenerOnFailureAnswer(Exception ex) {
            this.ex = ex;
        }

        @Override
        public Void answer(InvocationOnMock invocation) {
            ActionListener<?> actionListener = invocation.getArgument(2);
            actionListener.onFailure(ex);
            return null;
        }
    }
}
