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
package com.floragunn.signals.watch;

import org.junit.Test;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.signals.watch.WatchInstanceIdService.createInstanceId;
import static com.floragunn.signals.watch.WatchInstanceIdService.extractParentGenericWatchId;
import static com.floragunn.signals.watch.WatchInstanceIdService.isPossibleGenericWatchInstanceId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WatchInstanceIdServiceTest {

    @Test
    public void shouldCreateWatchInstanceId() {
        assertThat(createInstanceId("watch-id-1", "generic-id-1"), equalTo("watch-id-1/instances/generic-id-1"));
        assertThat(createInstanceId("two", "222_22"), equalTo("two/instances/222_22"));
        assertThat(createInstanceId("3...", "three"), equalTo("3.../instances/three"));
        assertThatThrown(() -> createInstanceId("incorrect/instances/watch-id", "instance-id"), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> createInstanceId("watch-id", "incorrect/instances/instance-id"), instanceOf(IllegalArgumentException.class));
    }

    @Test
    public void shouldReturnWatchId() {
        assertThatThrown(() -> extractParentGenericWatchId("regular_watch_id"), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> extractParentGenericWatchId("regular_watch_id"), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> extractParentGenericWatchId("watch@no/generic2"), instanceOf(IllegalArgumentException.class));
        assertThatThrown(() -> extractParentGenericWatchId("2000"), instanceOf(IllegalArgumentException.class));
    }


    @Test
    public void shouldExtractWatchId() {
        assertThat(extractParentGenericWatchId("2/instances/2=4"), equalTo("2"));
        assertThat(extractParentGenericWatchId("2/instances/3+4"), equalTo("2"));
        assertThat(extractParentGenericWatchId("watch@no/generic2/instances/instance_id"), equalTo("watch@no/generic2"));
        assertThat(extractParentGenericWatchId("watchId/instances/123210"), equalTo("watchId"));
        assertThat(extractParentGenericWatchId("watchId5/instances/+/instances/five"), equalTo("watchId5"));
    }

    @Test
    public void shouldBeWatchInstanceIdPossibly() {
        assertThat(isPossibleGenericWatchInstanceId("generic-watch-id/instances/instance-id"), equalTo(true));
        assertThat(isPossibleGenericWatchInstanceId("another/instances/watch-id"), equalTo(true));
        assertThat(isPossibleGenericWatchInstanceId("/instances//instances//instances/"), equalTo(true));
        assertThat(isPossibleGenericWatchInstanceId("++watch/instances/instance"), equalTo(true));
    }

    @Test
    public void shouldNotBeWatchInstanceIdPossibly() {
        assertThat(isPossibleGenericWatchInstanceId("generic-watch-id+instance-id"), equalTo(false));
        assertThat(isPossibleGenericWatchInstanceId("another+-watch-id"), equalTo(false));
        assertThat(isPossibleGenericWatchInstanceId("--instances----/INSTANCES/--/Instances/"), equalTo(false));
        assertThat(isPossibleGenericWatchInstanceId("++watch-/instance"), equalTo(false));
    }

}