package com.floragunn.signals.watch;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.signals.actions.watch.generic.service.WatchInstancesLoader;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.init.WatchInitializationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_ENABLED;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_INSTANCE_ID;
import static com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData.FIELD_PARAMETERS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GenericWatchInstanceFactoryVersionTest {

    public static final String WATCH_ID_1 = "watch-id-00001";
    @Mock
    private WatchInstancesLoader instancesLoader;

    @Mock
    private WatchInitializationService initService;
    @Mock
    private ThrottlePeriodParser throttlePeriodParser;

    private GenericWatchInstanceFactory genericWatchInstanceFactory;

    @Before
    public void before() {
        when(initService.getThrottlePeriodParser()).thenReturn(throttlePeriodParser);
        this.genericWatchInstanceFactory = new GenericWatchInstanceFactory(instancesLoader, initService);
    }

    @Test
    public void shouldCreateVersionNumber() {
        Watch watch = givenWatchAndParametersWithVersions(0, 0);

        ImmutableList<Watch> watches = genericWatchInstanceFactory.instantiateGeneric(watch);

        assertThat(watches, hasSize(1));
        Watch watchInstance = watches.get(0);
        assertThat(watchInstance.getVersion(), equalTo(0L));
    }

    @Test
    public void shouldUseParameterVersionToGenerateInstanceVersion() {
        Watch watch = givenWatchAndParametersWithVersions(0, 3);

        ImmutableList<Watch> watches = genericWatchInstanceFactory.instantiateGeneric(watch);

        assertThat(watches.get(0).getVersion(), equalTo(3L));
    }

    @Test
    public void shouldUseAnotherParameterVersionToGenerateInstanceVersion() {
        Watch watch = givenWatchAndParametersWithVersions(0, 5);

        ImmutableList<Watch> watches = genericWatchInstanceFactory.instantiateGeneric(watch);

        assertThat(watches.get(0).getVersion(), equalTo(5L));
    }

    @Test
    public void shouldUseGenericWatchVersionAsPartOfInstanceVersion() {
        Watch watch = givenWatchAndParametersWithVersions(1, 0);

        ImmutableList<Watch> watches = genericWatchInstanceFactory.instantiateGeneric(watch);

        assertThat(watches.get(0).getVersion(), greaterThanOrEqualTo((long)Math.pow(2, 32)));
    }

    @Test
    public void shouldDetectGenericVersionUpgrade() {
        long versionBeforeUpdate = versionForWatchAndParameters(1, 0);
        long versionAfterUpdate = versionForWatchAndParameters(2, 0);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectGenericVersionUpgrade_2() {
        long versionBeforeUpdate = versionForWatchAndParameters(5, 0);
        long versionAfterUpdate = versionForWatchAndParameters(6, 0);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectGenericVersionUpgrade_3() {
        long versionBeforeUpdate = versionForWatchAndParameters(300, 0);
        long versionAfterUpdate = versionForWatchAndParameters(301, 0);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectGenericVersionUpgrade_4() {
        long versionBeforeUpdate = versionForWatchAndParameters(7, 0);
        long versionAfterUpdate = versionForWatchAndParameters(2500, 0);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectParameterVersionUpgrade_1() {
        long versionBeforeUpdate = versionForWatchAndParameters(9, 4);
        long versionAfterUpdate = versionForWatchAndParameters(9, 5);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectParameterVersionUpgrade_2() {
        long versionBeforeUpdate = versionForWatchAndParameters(10, 5);
        long versionAfterUpdate = versionForWatchAndParameters(10, 6);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectParameterVersionUpgrade_3() {
        long versionBeforeUpdate = versionForWatchAndParameters(10, 6);
        long versionAfterUpdate = versionForWatchAndParameters(10, 7);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectParameterVersionUpgrade_4() {
        long versionBeforeUpdate = versionForWatchAndParameters(10, 2500);
        long versionAfterUpdate = versionForWatchAndParameters(10, 2501);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldDetectParameterVersionUpgrade_5() {
        long versionBeforeUpdate = versionForWatchAndParameters(10, 2500);
        long versionAfterUpdate = versionForWatchAndParameters(10, 3000);

        assertThat(versionAfterUpdate, greaterThan(versionBeforeUpdate));
    }

    @Test
    public void shouldBeMonotonic() {
        long version1 = versionForWatchAndParameters(0, 0);
        long version2 = versionForWatchAndParameters(0, 1);
        long version3 = versionForWatchAndParameters(0, 2);
        long version4 = versionForWatchAndParameters(1, 2);
        long version5 = versionForWatchAndParameters(2, 2);
        long version6 = versionForWatchAndParameters(3, 2);
        long version7 = versionForWatchAndParameters(4, 2);
        long version8 = versionForWatchAndParameters(5, 3);
        long version9 = versionForWatchAndParameters(5, 4);
        long version10 = versionForWatchAndParameters(5, 5);
        long version11 = versionForWatchAndParameters(5, 6);
        long version12 = versionForWatchAndParameters(5, 7);
        long version13 = versionForWatchAndParameters(5, 9);
        long version14 = versionForWatchAndParameters(7, 9);
        long version15 = versionForWatchAndParameters(9, 11);
        long version16 = versionForWatchAndParameters(9, 135_000);

        assertThat(version2, greaterThan(version1));
        assertThat(version3, greaterThan(version2));
        assertThat(version4, greaterThan(version3));
        assertThat(version5, greaterThan(version4));
        assertThat(version6, greaterThan(version5));
        assertThat(version7, greaterThan(version6));
        assertThat(version8, greaterThan(version7));
        assertThat(version9, greaterThan(version8));
        assertThat(version10, greaterThan(version9));
        assertThat(version11, greaterThan(version10));
        assertThat(version12, greaterThan(version11));
        assertThat(version13, greaterThan(version12));
        assertThat(version14, greaterThan(version13));
        assertThat(version15, greaterThan(version14));
        assertThat(version16, greaterThan(version15));
    }

    private long versionForWatchAndParameters(int watchVersion, int parametersVersion) {
        Watch watch = givenWatchAndParametersWithVersions(watchVersion, parametersVersion);
        return genericWatchInstanceFactory.instantiateGeneric(watch).get(0).getVersion();
    }

    private Watch givenWatchAndParametersWithVersions(long watchVersion, int parametersVersion) {
        try {
            Watch watch = new WatchBuilder(WATCH_ID_1).instances(true).cronTrigger("0 0 0 1 1 ?")//
                .search("source-search-index").query("{\"match_all\" : {} }").as("testsearch")//
                .then().index("testsink").throttledFor("1h").name("testsink").build();
            String watchJson = watch.toJson();
            watch = Watch.parse(initService, "tenant-id", WATCH_ID_1, watchJson, watchVersion, null);
            DocNode docNode = DocNode.of(FIELD_INSTANCE_ID, "instance-id", FIELD_ENABLED, true, FIELD_PARAMETERS, DocNode.EMPTY);
            WatchInstanceData watchInstanceData = new WatchInstanceData(docNode, parametersVersion);
            when(instancesLoader.findInstances(WATCH_ID_1)).thenReturn(ImmutableList.of(watchInstanceData));
            return watch;
        } catch ( ConfigValidationException | IOException e) {
            throw new RuntimeException("Cannot create watch or instance parameters.", e);
        }
    }
}