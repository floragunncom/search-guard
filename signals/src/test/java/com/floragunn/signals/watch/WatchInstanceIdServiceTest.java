package com.floragunn.signals.watch;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class WatchInstanceIdServiceTest {

    private final WatchInstanceIdService service = WatchInstanceIdService.INSTANCE;

    @Test
    public void shouldCreateWatchInstanceId_one() {
        String instanceId = service.createInstanceId("watch-id-1", "generic-id-1");

        assertThat(instanceId, equalTo("watch-id-1+generic-id-1"));
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
        String watchId = service.extractGenericWatchOrWatchId("regular_watch_id");

        assertThat(watchId, equalTo("regular_watch_id"));
    }

    @Test
    public void shouldReturnWatchId_two() {
        String watchId = service.extractGenericWatchOrWatchId("watch@no/generic2");

        assertThat(watchId, equalTo("watch@no/generic2"));
    }

    @Test
    public void shouldReturnWatchId_three() {
        String watchId = service.extractGenericWatchOrWatchId("2000");

        assertThat(watchId, equalTo("2000"));
    }

    @Test
    public void shouldExtractWatchId_one() {
        String watchId = service.extractGenericWatchOrWatchId("2+2=4");

        assertThat(watchId, equalTo("2"));
    }

    @Test
    public void shouldExtractWatchId_two() {
        String watchId = service.extractGenericWatchOrWatchId("2+3+4");

        assertThat(watchId, equalTo("2"));
    }

    @Test
    public void shouldExtractWatchId_three() {
        String watchId = service.extractGenericWatchOrWatchId("watch@no/generic2+instance_id");

        assertThat(watchId, equalTo("watch@no/generic2"));
    }

    @Test
    public void shouldExtractWatchId_four() {
        String watchId = service.extractGenericWatchOrWatchId("watchId+123210");

        assertThat(watchId, equalTo("watchId"));
    }

    @Test
    public void shouldExtractWatchId_five() {
        String watchId = service.extractGenericWatchOrWatchId("watchId5++++++five");

        assertThat(watchId, equalTo("watchId5"));
    }
}