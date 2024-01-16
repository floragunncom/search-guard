package com.floragunn.signals.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.signals.execution.WatchExecutionContextData.TriggerInfo;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Test;

public class WatchExecutionContextDataTest {

    private static final ZonedDateTime TRIGGERED = ZonedDateTime.now();
    private static final ZonedDateTime SCHEDULED = ZonedDateTime.now().minusMinutes(1);
    private static final ZonedDateTime PREVIOUS = ZonedDateTime.now().minusMinutes(2);
    private static final ZonedDateTime NEXT = ZonedDateTime.now().plusMinutes(1);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_ZONED_DATE_TIME;

    @Test
    public void testToXContent() throws IOException, DocumentParseException {
        // Given
        TriggerInfo triggerInfo = new TriggerInfo(TRIGGERED, SCHEDULED, PREVIOUS, NEXT);

        // When
        XContentBuilder xContent = triggerInfo.toXContent(XContentFactory.jsonBuilder(), null);

        // Then
        DocNode docNode = DocNode.parse(Format.JSON).from(Strings.toString(xContent));
        assertThat(TRIGGERED.format(FORMATTER), equalTo(docNode.get("triggered_time")));
        assertThat(SCHEDULED.format(FORMATTER), equalTo(docNode.get("scheduled_time")));
        assertThat(PREVIOUS.format(FORMATTER), equalTo(docNode.get("previous_scheduled_time")));
        assertThat(NEXT.format(FORMATTER), equalTo(docNode.get("next_scheduled_time")));
    }

    @Test
    public void testToXContentWithNullValues() throws IOException, DocumentParseException {
        // Given
        TriggerInfo triggerInfo = new TriggerInfo();

        // When
        XContentBuilder xContent = triggerInfo.toXContent(XContentFactory.jsonBuilder(), null);

        // Then
        DocNode docNode = DocNode.parse(Format.JSON).from(Strings.toString(xContent));
        assertThat(docNode.get("triggered_time"), is(nullValue()));
        assertThat(docNode.get("scheduled_time"), is(nullValue()));
        assertThat(docNode.get("previous_scheduled_time"), is(nullValue()));
        assertThat(docNode.get("next_scheduled_time"), is(nullValue()));
    }

    @Test
    public void testToXContentWithSomeValuesMissing() throws IOException, DocumentParseException {
        // Given
        TriggerInfo triggerInfo = new TriggerInfo(TRIGGERED, null, PREVIOUS, null);

        // When
        XContentBuilder xContent = triggerInfo.toXContent(XContentFactory.jsonBuilder(), null);

        // Then
        DocNode docNode = DocNode.parse(Format.JSON).from(Strings.toString(xContent));
        assertThat(TRIGGERED.format(FORMATTER), equalTo(docNode.get("triggered_time")));
        assertThat(PREVIOUS.format(FORMATTER), equalTo(docNode.get("previous_scheduled_time")));
        assertThat(docNode.get("scheduled_time"), is(nullValue()));
        assertThat(docNode.get("next_scheduled_time"), is(nullValue()));
    }
}