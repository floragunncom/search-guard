package com.floragunn.signals.watch.common;

import com.floragunn.fluent.collections.ImmutableList;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
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