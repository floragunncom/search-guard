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

import com.floragunn.codova.config.temporal.ConstantDurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.jobs.cluster.CurrentNodeJobSelector;
import com.floragunn.signals.actions.watch.generic.service.WatchInstancesLoader;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.init.WatchInitializationService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_ENABLED;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_INSTANCE_ID;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_PARAMETERS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WatchTest {

    public static final String WATCH_ID = "watch-id";
    public static final String INSTANCE_ID_1 = "instance-id-1";
    public static final String INSTANCE_ID_2 = "instance-id-2";
    @Mock
    private WatchInitializationService initService;
    @Mock
    private CurrentNodeJobSelector jobNodeSelector;
    @Mock
    private WatchInstancesLoader instanceDataLoader;
    @Mock
    private ThrottlePeriodParser throttlePeriodParser;

    private GenericWatchInstanceFactory factory;


    @Before
    public void before() {
        this.factory = new GenericWatchInstanceFactory(instanceDataLoader, initService, jobNodeSelector);
    }

    @Test
    public void shouldBeExecutableAndNotGeneric() throws ParseException, IOException {
        Watch watch = new WatchBuilder(WATCH_ID)//
            .atMsInterval(25).search("index") //
            .query("{\"match_all\" : {} }").as("testsearch") //
            .then().index("destination-index")
            .name("testsink").build();

        assertThat(watch.isExecutable(), equalTo(true));
        assertThat(watch.isGenericJobConfig(), equalTo(false));
    }

    @Test
    public void shouldBeNotExecutableAndGeneric() throws ParseException, IOException {
        Watch watch = new WatchBuilder(WATCH_ID).instances(true)//
            .atMsInterval(25).search("index") //
            .query("{\"match_all\" : {} }").as("testsearch") //
            .then().index("destination-index")
            .name("testsink").build();

        assertThat(watch.isExecutable(), equalTo(false));
        assertThat(watch.isGenericJobConfig(), equalTo(true));
    }

    @Test
    public void shouldBeExecutableAndNotGenericAsWatchInstance() throws IOException, ConfigValidationException {
        when(throttlePeriodParser.getDefaultThrottle()).thenReturn(new ConstantDurationExpression(Duration.ofDays(1)));
        when(initService.getThrottlePeriodParser()).thenReturn(throttlePeriodParser);
        when(jobNodeSelector.isJobSelected(any(Watch.class))).thenReturn(true);
        Watch watch = new WatchBuilder(WATCH_ID).instances(true)//
            .atMsInterval(25).search("index") //
            .query("{\"match_all\" : {} }").as("testsearch") //
            .then().index("destination-index") //
            .name("testsink").build();
        watch = makeGenericWatch(watch);
        DocNode docNode = DocNode.of(FIELD_INSTANCE_ID, INSTANCE_ID_1, FIELD_ENABLED, true, FIELD_PARAMETERS, DocNode.EMPTY);
        WatchInstanceData watchInstanceData = new WatchInstanceData(docNode, 0);
        when(instanceDataLoader.findInstances(WATCH_ID)).thenReturn(ImmutableList.of(watchInstanceData));

        ImmutableList<Watch> watches = factory.instantiateGeneric(watch);

        assertThat(watches, hasSize(1));
        Watch genericWatchInstance = watches.get(0);
        assertThat(genericWatchInstance.isGenericJobConfig(), equalTo(false));
        assertThat(genericWatchInstance.isExecutable(), equalTo(true));
    }

    @Test
    public void shouldCreateCompoundIdForWatchesInstancesAndCorrectType() throws IOException, ConfigValidationException {
        when(throttlePeriodParser.getDefaultThrottle()).thenReturn(new ConstantDurationExpression(Duration.ofDays(1)));
        when(initService.getThrottlePeriodParser()).thenReturn(throttlePeriodParser);
        when(jobNodeSelector.isJobSelected(any(Watch.class))).thenReturn(true);
        Watch watch = new WatchBuilder(WATCH_ID).instances(true)//
            .atMsInterval(25).search("index") //
            .query("{\"match_all\" : {} }").as("testsearch") //
            .then().index("destination-index") //
            .name("testsink").build();
        watch = makeGenericWatch(watch);
        DocNode docNodeOne = DocNode.of(FIELD_INSTANCE_ID, INSTANCE_ID_1, FIELD_ENABLED, true, FIELD_PARAMETERS, DocNode.EMPTY);
        DocNode docNodeTwo = DocNode.of(FIELD_INSTANCE_ID, INSTANCE_ID_2, FIELD_ENABLED, true, FIELD_PARAMETERS, DocNode.EMPTY);
        WatchInstanceData watchInstanceDataOne = new WatchInstanceData(docNodeOne, 0);
        WatchInstanceData watchInstanceDataTwo = new WatchInstanceData(docNodeTwo, 0);
        when(instanceDataLoader.findInstances(WATCH_ID)).thenReturn(ImmutableList.of(watchInstanceDataOne, watchInstanceDataTwo));

        ImmutableList<Watch> watches = factory.instantiateGeneric(watch);

        assertThat(watches, hasSize(2));
        Watch genericWatchInstance = watches.get(0);
        final String genericWatchInstanceIdSeparator = "/instances/";
        assertThat(genericWatchInstance.getId(), equalTo(WATCH_ID + genericWatchInstanceIdSeparator + INSTANCE_ID_1));
        assertThat(genericWatchInstance.isExecutable(), equalTo(true));
        assertThat(genericWatchInstance.isGenericJobConfig(), equalTo(false));
        genericWatchInstance = watches.get(1);
        assertThat(genericWatchInstance.getId(), equalTo(WATCH_ID + genericWatchInstanceIdSeparator + INSTANCE_ID_2));
        assertThat(genericWatchInstance.isExecutable(), equalTo(true));
        assertThat(genericWatchInstance.isGenericJobConfig(), equalTo(false));
    }

    @Test
    public void shouldNotCreateGenericWatchWithGenericWatchInstanceSeparatorInId() throws IOException {
        final String watchInstanceSeparator = "/instances/";
        when(throttlePeriodParser.getDefaultThrottle()).thenReturn(new ConstantDurationExpression(Duration.ofDays(1)));
        when(initService.getThrottlePeriodParser()).thenReturn(throttlePeriodParser);
        Watch watch = new WatchBuilder(WATCH_ID + "with-not-separator-" + watchInstanceSeparator).instances(true)//
            .atMsInterval(25).search("index") //
            .query("{\"match_all\" : {} }").as("testsearch") //
            .then().index("destination-index") //
            .name("testsink").build();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().humanReadable(true);
        watch.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String watchJson = BytesReference.bytes(builder).utf8ToString();

        ConfigValidationException ex = (ConfigValidationException) assertThatThrown(
            () -> Watch.parse(initService, "_main", watch.getId(), watchJson, 7L),
            instanceOf(ConfigValidationException.class));

        assertThat(ex.getMessage(), containsString("id cannot contain '" + watchInstanceSeparator + "'")); //
    }

    private Watch makeGenericWatch(Watch watch) throws IOException, ConfigValidationException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().humanReadable(true);
        watch.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String watchJson = BytesReference.bytes(builder).utf8ToString();
        watch = Watch.parse(initService, "_main", watch.getId(), watchJson, 3L);
        assertThat(watch.isGenericJobConfig(), equalTo(true));
        assertThat(watch.isExecutable(), equalTo(false));
        return watch;
    }
}