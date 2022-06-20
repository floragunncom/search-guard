/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.signals.watch.state;

import org.elasticsearch.common.Strings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.severity.SeverityLevel;

public class WatchStateTest {

    @Test
    public void parse() throws Exception {
        WatchState watchState = new WatchState("test_tenant", "test_node");
        watchState.setLastStatus(new Status(Status.Code.ACTION_EXECUTED, SeverityLevel.CRITICAL, "test_detail"));
        String watchStateJson = Strings.toString(watchState);

        WatchState parsedWatchState = WatchState.createFromJson("test_tenant", watchStateJson);

        Assert.assertEquals(watchState.getNode(), parsedWatchState.getNode());
        Assert.assertEquals(watchState.getTenant(), parsedWatchState.getTenant());
        Assert.assertEquals(watchState.getLastSeverityLevel(), parsedWatchState.getLastSeverityLevel());
    }
}
