package com.floragunn.signals.watch;

import org.junit.Test;

import static com.floragunn.signals.watch.WatchInstanceIdService.createInstanceId;
import static com.floragunn.signals.watch.WatchInstanceIdService.extractGenericWatchOrWatchId;
import static com.floragunn.signals.watch.WatchInstanceIdService.extractInstanceId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class WatchInstanceIdServiceTest {


    @Test
    public void shouldCreateWatchInstanceId() {
        assertThat(createInstanceId("watch-id-1", "generic-id-1"), equalTo("watch-id-1+generic-id-1"));
        assertThat(createInstanceId("two", "222_22"), equalTo("two+222_22"));
        assertThat(createInstanceId("3...", "three"), equalTo("3...+three"));
    }

    @Test
    public void shouldReturnWatchId() {
        assertThat(extractGenericWatchOrWatchId("regular_watch_id"), equalTo("regular_watch_id"));
        assertThat(extractGenericWatchOrWatchId("watch@no/generic2"), equalTo("watch@no/generic2"));
        assertThat(extractGenericWatchOrWatchId("2000"), equalTo("2000"));
    }


    @Test
    public void shouldExtractWatchId() {
        assertThat(extractGenericWatchOrWatchId("2+2=4"), equalTo("2"));
        assertThat(extractGenericWatchOrWatchId("2+3+4"), equalTo("2"));
        assertThat(extractGenericWatchOrWatchId("watch@no/generic2+instance_id"), equalTo("watch@no/generic2"));
        assertThat(extractGenericWatchOrWatchId("watchId+123210"), equalTo("watchId"));
        assertThat(extractGenericWatchOrWatchId("watchId5++++++five"), equalTo("watchId5"));
    }

    @Test
    public void shouldExtractInstanceId() {
        assertThat(extractInstanceId("2plus2=4").isPresent(), equalTo(false));
        assertThat(extractInstanceId("no_instance_id+").isPresent(), equalTo(false));
        assertThat(extractInstanceId("2+2=4").get(), equalTo("2=4"));
        assertThat(extractInstanceId("2+3+4").get(), equalTo("3+4"));
        assertThat(extractInstanceId("watch@no/generic2+instance_id").get(), equalTo("instance_id"));
        assertThat(extractInstanceId("watchId+123210").get(), equalTo("123210"));
        assertThat(extractInstanceId("watchId5++++++five").get(), equalTo("+++++five"));
    }

}