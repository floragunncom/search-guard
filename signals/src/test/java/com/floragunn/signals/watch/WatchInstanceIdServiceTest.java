package com.floragunn.signals.watch;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class WatchInstanceIdServiceTest {

    private final WatchInstanceIdService service = WatchInstanceIdService.INSTANCE;

    @Test
    public void shouldCreateWatchInstanceId_one() {
        String instanceId = service.createInstanceId("watch-id-1", "template-id-1");

        assertThat(instanceId, equalTo("watch-id-1+template-id-1"));
    }

    @Test
    public void shouldCreateWatchInstanceId_two() {
        String instanceId = service.createInstanceId("two", "222_22");

        assertThat(instanceId, equalTo("two+222_22"));
    }

    @Test
    public void shouldCreateWatchInstanceId_three() {
        String instanceId = service.createInstanceId("3...", "three");

        assertThat(instanceId, equalTo("3...+three"));
    }

    @Test
    public void shouldReturnWatchId_one() {
        String templateId = service.getWatchTemplateId("regular_watch_id");

        assertThat(templateId, equalTo("regular_watch_id"));
    }

    @Test
    public void shouldReturnWatchId_two() {
        String templateId = service.getWatchTemplateId("watch@no/template2");

        assertThat(templateId, equalTo("watch@no/template2"));
    }

    @Test
    public void shouldReturnWatchId_three() {
        String templateId = service.getWatchTemplateId("2000");

        assertThat(templateId, equalTo("2000"));
    }

    @Test
    public void shouldExtractWatchId_one() {
        String templateId = service.getWatchTemplateId("2+2=4");

        assertThat(templateId, equalTo("2"));
    }

    @Test
    public void shouldExtractWatchId_two() {
        String templateId = service.getWatchTemplateId("2+3+4");

        assertThat(templateId, equalTo("2"));
    }

    @Test
    public void shouldExtractWatchId_three() {
        String templateId = service.getWatchTemplateId("watch@no/template2+instance_id");

        assertThat(templateId, equalTo("watch@no/template2"));
    }

    @Test
    public void shouldExtractWatchId_four() {
        String templateId = service.getWatchTemplateId("watchId+123210");

        assertThat(templateId, equalTo("watchId"));
    }

    @Test
    public void shouldExtractWatchId_five() {
        String templateId = service.getWatchTemplateId("watchId5++++++five");

        assertThat(templateId, equalTo("watchId5"));
    }
}