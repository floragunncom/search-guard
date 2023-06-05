/*
 * Copyright 2019-2023 floragunn GmbH
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
package com.floragunn.signals.actions.watch.execute;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;
import com.floragunn.signals.actions.watch.execute.ExecuteGenericWatchAction.ExecuteGenericWatchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExecuteGenericWatchActionTest {

    public static final String TENANT_1 = "tenant-1";
    public static final String TENANT_2 = "tenant-2";
    public static final String WATCH_ID_1 = "watch-id-1";
    public static final String WATCH_ID_2 = "watch-id-2";

    public static final String RESULT_1 = "result one";
    public static final String RESULT_2 = "result two";
    @Mock
    private UnparsedMessage unparsedMessage;

    @Test
    public void shouldSerializeAndDeserializeByteReferenceInExecuteWatchResponse_1() throws ConfigValidationException {
        BytesReference bytesArray = new BytesArray(RESULT_1.getBytes(StandardCharsets.UTF_8));
        ExecuteGenericWatchResponse response = new ExecuteGenericWatchResponse(TENANT_1, WATCH_ID_1, null, bytesArray);
        byte[] serialized = DocNode.wrap(response.toBasicObject()).toSmile();
        when(unparsedMessage.requiredDocNode()).thenReturn(DocNode.parse(Format.SMILE).from(serialized));
        when(unparsedMessage.getMetaDataDocNode()).thenReturn(DocNode.of("status", 200));

        ExecuteGenericWatchResponse deserialized = new ExecuteGenericWatchResponse(unparsedMessage);

        assertThat(deserialized.getExecutionStatus(), nullValue());
        assertThat(deserialized.getTenant(), equalTo(TENANT_1));
        assertThat(deserialized.getId(), equalTo(WATCH_ID_1));
        BytesReference result = deserialized.getResult();
        assertThat(result, notNullValue());
        assertThat(new String(result.array(), StandardCharsets.UTF_8), equalTo(RESULT_1));
    }

    @Test
    public void shouldSerializeAndDeserializeByteReferenceInExecuteWatchResponse_2() throws ConfigValidationException {
        BytesReference bytesArray = new BytesArray(RESULT_2.getBytes(StandardCharsets.UTF_8));
        ExecuteGenericWatchResponse response = new ExecuteGenericWatchResponse(TENANT_2, WATCH_ID_2, null, bytesArray);
        byte[] serialized = DocNode.wrap(response.toBasicObject()).toSmile();
        when(unparsedMessage.requiredDocNode()).thenReturn(DocNode.parse(Format.SMILE).from(serialized));
        when(unparsedMessage.getMetaDataDocNode()).thenReturn(DocNode.of("status", 200));

        ExecuteGenericWatchResponse deserialized = new ExecuteGenericWatchResponse(unparsedMessage);

        assertThat(deserialized.getExecutionStatus(), nullValue());
        assertThat(deserialized.getTenant(), equalTo(TENANT_2));
        assertThat(deserialized.getId(), equalTo(WATCH_ID_2));
        BytesReference result = deserialized.getResult();
        assertThat(result, notNullValue());
        assertThat(new String(result.array(), StandardCharsets.UTF_8), equalTo(RESULT_2));
    }
}