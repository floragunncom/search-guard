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
package com.floragunn.signals.watch.common;

import com.floragunn.fluent.collections.ImmutableList;
import org.junit.Test;

import java.util.Arrays;

import static com.floragunn.signals.watch.common.Instances.EMPTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class InstancesTest {

    @Test
    public void shouldCheckIfParameterListsAreTheSame() {
        assertThat(EMPTY.hasSameParameterList(EMPTY), equalTo(true));
        assertThat(EMPTY.hasSameParameterList(instanceWithParams()), equalTo(true));
        assertThat(EMPTY.hasSameParameterList(instanceWithParams("one")), equalTo(false));
        assertThat(EMPTY.hasSameParameterList(instanceWithParams("one", "two")), equalTo(false));
        assertThat(instanceWithParams("one").hasSameParameterList(instanceWithParams("one")), equalTo(true));
        assertThat(instanceWithParams("one", "two").hasSameParameterList(instanceWithParams("one")), equalTo(false));
        assertThat(instanceWithParams("one", "two").hasSameParameterList(instanceWithParams("two", "one")), equalTo(true));
        assertThat(instanceWithParams("one", "two").hasSameParameterList(instanceWithParams("two", "one", "3")), equalTo(false));
        assertThat(instanceWithParams("3", "one", "two").hasSameParameterList(instanceWithParams("two", "one", "3")), equalTo(true));
        assertThat(instanceWithParams("3").hasSameParameterList(instanceWithParams("two", "one", "3")), equalTo(false));
    }

    private Instances instanceWithParams(String...parameterNames) {
        ImmutableList<String> params = ImmutableList.of(Arrays.asList(parameterNames));
        return new Instances(true, params);
    }
}