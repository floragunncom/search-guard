package com.floragunn.signals.watch;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.jobs.cluster.CurrentNodeJobSelector;
import com.floragunn.signals.actions.watch.generic.service.WatchInstancesLoader;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.init.WatchInitializationService;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.text.ParseException;

import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_ENABLED;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_INSTANCE_ID;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_PARAMETERS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WatchTest {

    public static final String WATCH_ID = "watch-id";
    public static final String INSTANCE_ID = "instance-id";
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
//        when(initService.getThrottlePeriodParser()).thenReturn(throttlePeriodParser);
//        when(jobNodeSelector.isJobSelected(any(Watch.class))).thenReturn(true);
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
    @Ignore
    public void shouldBeExecutableAndNotGenericAsWatchInstance() throws ParseException, IOException {
        Watch watch = new WatchBuilder(WATCH_ID).instances(true)//
            .atMsInterval(25).search("index") //
            .query("{\"match_all\" : {} }").as("testsearch") //
            .then().index("destination-index")
            .name("testsink").build();
        DocNode docNode = DocNode.of(FIELD_INSTANCE_ID, INSTANCE_ID, FIELD_ENABLED, true, FIELD_PARAMETERS, DocNode.EMPTY);
        WatchInstanceData watchInstanceData = new WatchInstanceData(docNode, 0);
        when(instanceDataLoader.findInstances(WATCH_ID)).thenReturn(ImmutableList.of(watchInstanceData));

        //TODO this line fails. The watch is not created because field com.floragunn.signals.watch.Watch.genericDefinition is missing.
        // Field is set only when the watch is parsed from json. So the watch before usage should be serialized and deserialized??
        ImmutableList<Watch> watches = factory.instantiateGeneric(watch);

        assertThat(watches, hasSize(1));
        Watch genericWatchInstance = watches.get(0);
        assertThat(genericWatchInstance.isGenericJobConfig(), equalTo(false));
        assertThat(genericWatchInstance.isExecutable(), equalTo(true));
    }
}