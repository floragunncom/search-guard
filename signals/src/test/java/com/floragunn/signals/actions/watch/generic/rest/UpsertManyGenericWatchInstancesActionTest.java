package com.floragunn.signals.actions.watch.generic.rest;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.actions.watch.generic.rest.UpsertManyGenericWatchInstancesAction.UpsertManyGenericWatchInstancesRequest;
import org.junit.Test;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;

public class UpsertManyGenericWatchInstancesActionTest {

    public static final String TENANT_ID_1 = "tenant-id-1";

    public static final String WATCH_ID_1 = "watch-id-1";

    @Test
    public void shouldReturnValidationErrorWhenWatchInstancesAreCreatedWithEmptyBody() {
        UnparsedDocument.StringDoc unparsedDocument = new UnparsedDocument.StringDoc(DocNode.EMPTY.toJsonString(), Format.JSON);

        ConfigValidationException ex = (ConfigValidationException)
            assertThatThrown(() -> new UpsertManyGenericWatchInstancesRequest(TENANT_ID_1, WATCH_ID_1, unparsedDocument), instanceOf(
                ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        String expectedMessage = "Request does not contain any definitions of watch instances.";
        assertThat(errors, containSubstring("$.body[0].error", expectedMessage));
    }
}