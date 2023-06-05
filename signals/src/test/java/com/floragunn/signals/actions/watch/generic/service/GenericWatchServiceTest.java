package com.floragunn.signals.actions.watch.generic.service;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument.StringDoc;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.actions.watch.generic.rest.DeleteWatchInstanceAction.DeleteWatchInstanceRequest;
import com.floragunn.signals.actions.watch.generic.rest.GetAllWatchInstancesAction.GetAllWatchInstancesRequest;
import com.floragunn.signals.actions.watch.generic.rest.GetWatchInstanceAction.GetWatchInstanceParametersRequest;
import com.floragunn.signals.actions.watch.generic.rest.UpsertManyGenericWatchInstancesAction.UpsertManyGenericWatchInstancesRequest;
import com.floragunn.signals.actions.watch.generic.rest.UpsertOneGenericWatchInstanceAction.UpsertOneGenericWatchInstanceRequest;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstancesRepository;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchStateRepository;
import com.floragunn.signals.watch.common.Instances;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.floragunn.searchsupport.junit.ThrowableAssert.assertThatThrown;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containSubstring;
import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GenericWatchServiceTest {

    private static final Logger log = LogManager.getLogger(GenericWatchServiceTest.class);

    public static final StringDoc EMPTY_DOCUMENT = new StringDoc("{}", Format.JSON);
    public static final String TENANT_ID_1 = "tenant-id-1";
    public static final String TENANT_ID_2 = "tenant-id-2";
    public static final String WATCH_ID_1 = "watch-id-1";
    public static final String WATCH_ID_2 = "watch-id-2";
    public static final String INSTANCE_ID_1 = "instance_id_1";
    public static final String INSTANCE_ID_2 = "instance_id_2";
    public static final String INSTANCE_ID_3 = "instance_id_3";
    public static final String INSTANCE_PARAM_1 = "param_1";
    public static final String INSTANCE_PARAM_2 = "param_2";
    public static final String INSTANCE_PARAM_3 = "param_3";
    public static final String INSTANCE_PARAM_4 = "param_4";
    @Mock
    private Signals signals;
    @Mock
    private WatchInstancesRepository instancesRepository;
    @Mock
    private WatchStateRepository stateRepository;
    @Mock
    private SchedulerConfigUpdateNotifier notifier;
    @Mock
    private SignalsTenant signalsTenant;
    @Captor
    private ArgumentCaptor<WatchInstanceData> watchInstanceDataCaptor;
    @Captor
    private ArgumentCaptor<Runnable> runnableCaptor;

    // under tests
    public GenericWatchService service;

    @Before
    public void setUp() {
        this.service = new GenericWatchService(signals, instancesRepository, stateRepository, notifier);
    }

    @Test
    public void shouldNotContainInstanceIdSeparatorInInstanceId()
        throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(new Instances(true, ImmutableList.empty())));
        when(signalsTenant.watchesExist()).thenReturn(ImmutableMap.empty());
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(
            TENANT_ID_1,
            WATCH_ID_1, "incorrect/instances/instanceId", EMPTY_DOCUMENT);

        ConfigValidationException ex =
            (ConfigValidationException) assertThatThrown(() -> service.upsert(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$['incorrect/instances/instanceId'][0].error", "Watch instance id is incorrect."));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldValidateInstanceId()
        throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(new Instances(true, ImmutableList.empty())));
        when(signalsTenant.watchesExist()).thenReturn(ImmutableMap.empty());
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(
            TENANT_ID_1,
            WATCH_ID_1, "invalid instance id", EMPTY_DOCUMENT);

        ConfigValidationException ex =
            (ConfigValidationException) assertThatThrown(() -> service.upsert(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$['invalid instance id'][0].error", "Watch instance id is incorrect."));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldNotCreateGenericWatchParametersWhenWatchDoesNotExist()
    throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.empty());
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(
            TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, EMPTY_DOCUMENT);

        StandardResponse response = service.upsert(request);

        log.debug("Create watch instance response '{}'", response);
        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldNotCreateGenericWatchParametersWhenWatchIsNotGeneric()
        throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(Instances.EMPTY));
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(
            TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, EMPTY_DOCUMENT);

        StandardResponse response = service.upsert(request);

        log.debug("Create watch instance response '{}'", response);
        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldCreateGenericWatchParameters_1() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1, false);
        when(signalsTenant.watchesExist(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1)).thenReturn(existingWatches);
        when(instancesRepository.findOneById(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1)).thenReturn(Optional.empty());
        DocNode node = DocNode.of(INSTANCE_PARAM_1, "one", INSTANCE_PARAM_2, 2);
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, new StringDoc(node.toJsonString(), Format.JSON));

        StandardResponse response = service.upsert(request);

        log.debug("Create watch instance response '{}'", response);
        assertThat(response.getStatus(), equalTo(SC_CREATED));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_1), any(Runnable.class));
        WatchInstanceData watchInstanceData = watchInstanceDataCaptor.getValue();
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_1));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, "one"));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, 2));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(2));
    }

    @Test
    public void shouldCreateGenericWatchParameters_2() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_2)).thenReturn(signalsTenant);
        Instances instances = new Instances(true, ImmutableList.of(INSTANCE_PARAM_2, INSTANCE_PARAM_3, INSTANCE_PARAM_4));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_2)).thenReturn(Optional.of(instances));
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_2 + "/instances/" + INSTANCE_ID_2, false);
        when(signalsTenant.watchesExist(WATCH_ID_2 + "/instances/" + INSTANCE_ID_2)).thenReturn(existingWatches);
        DocNode node = DocNode.of(INSTANCE_PARAM_2, "two", INSTANCE_PARAM_3, "three", INSTANCE_PARAM_4, false);
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2,
            new StringDoc(node.toJsonString(), Format.JSON));

        StandardResponse response = service.upsert(request);

        log.debug("Create watch instance response '{}'", response);
        assertThat(response.getStatus(), equalTo(SC_CREATED));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_2), any(Runnable.class));
        WatchInstanceData watchInstanceData = watchInstanceDataCaptor.getValue();
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_2));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, "two"));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_3, "three"));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_4, false));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(3));
    }

    @Test
    public void shouldNotCreateGenericWatchInstanceWithTooManyParameters() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        DocNode node = DocNode.of(INSTANCE_PARAM_1, "one", INSTANCE_PARAM_2, "this parameter should cause validation error");
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, new StringDoc(node.toJsonString(), Format.JSON));

        ConfigValidationException ex =
            (ConfigValidationException) assertThatThrown(() -> service.upsert(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$.instance_id_1[0].error", "Incorrect parameter names: ['param_2']. Valid parameter names: ['param_1']"));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldNotCreateGenericWatchInstanceWithMissingParameters() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2, INSTANCE_PARAM_3));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        DocNode node = DocNode.of(INSTANCE_PARAM_1, "two parameters are missing");
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_2, new StringDoc(node.toJsonString(), Format.JSON));

        ConfigValidationException ex =
            (ConfigValidationException) assertThatThrown(() -> service.upsert(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$.instance_id_2[0].error", "Watch instance does not contain required parameters: ['param_2', 'param_3']"));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldNotUseNestedParameters() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of("map_parameter"));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        DocNode node = DocNode.of("map_parameter", ImmutableMap.of("map", "is", "not", "allowed"));
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, new StringDoc(node.toJsonString(), Format.JSON));

        ConfigValidationException ex = (ConfigValidationException) assertThatThrown(
            () -> service.upsert(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$['instance_id_1.map_parameter'][0].error", "Forbidden parameter value type"));
        assertThat(errors, containSubstring("$['instance_id_1.map_parameter'][0].error", "Map"));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldNotUseNestedValuesInList() throws NoSuchTenantException, SignalsUnavailableException, ConfigValidationException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of("list"));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        ImmutableMap<String, String> firstMap = ImmutableMap.of("one-key", "one-value", "two-key", "two-value");
        ImmutableMap<String, Integer> secondMap = ImmutableMap.of("three-key", 3, "four-key", 4);
        DocNode node = DocNode.of("list", Arrays.asList("one", firstMap, secondMap));
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, new StringDoc(node.toJsonString(), Format.JSON));

        ConfigValidationException ex = (ConfigValidationException) assertThatThrown(
            () -> service.upsert(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(2));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$['instance_id_1.list[1]'][0].error", "Forbidden parameter value type"));
        assertThat(errors, containSubstring("$['instance_id_1.list[1]'][0].error", "Map"));
        assertThat(errors, containSubstring("$['instance_id_1.list[2]'][0].error", "Forbidden parameter value type"));
        assertThat(errors, containSubstring("$['instance_id_1.list[2]'][0].error", "Map"));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldUpdateWatchInstance() throws NoSuchTenantException, SignalsUnavailableException, ConfigValidationException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1, false);
        when(signalsTenant.watchesExist(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1)).thenReturn(existingWatches);
        DocNode node = DocNode.of(INSTANCE_PARAM_1, "one", INSTANCE_PARAM_2, 2);
        ImmutableMap<String, Object> olderParams = ImmutableMap.of(INSTANCE_PARAM_1, "old value 1", INSTANCE_PARAM_2, "old value 2");
        WatchInstanceData instanceDataToBeUpdated = new WatchInstanceData(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, true, olderParams);
        when(instancesRepository.findOneById(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1)).thenReturn(Optional.of(instanceDataToBeUpdated));
        UpsertOneGenericWatchInstanceRequest request = new UpsertOneGenericWatchInstanceRequest(TENANT_ID_1,
            WATCH_ID_1, INSTANCE_ID_1, new StringDoc(node.toJsonString(), Format.JSON));

        StandardResponse response = service.upsert(request);

        log.debug("Create watch instance response '{}'", response);
        assertThat(response.getStatus(), equalTo(SC_OK));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_1), any(Runnable.class));
        WatchInstanceData watchInstanceData = watchInstanceDataCaptor.getValue();
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_1));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, "one"));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, 2));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(2));
    }

    @Test
    public void shouldDeleteWatchInstance_1() {
        when(instancesRepository.delete(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1)).thenReturn(true);

        StandardResponse response = service.deleteWatchInstance(new DeleteWatchInstanceRequest(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1));

        assertThat(response.getStatus(), equalTo(SC_OK));
        verify(notifier).send(eq(TENANT_ID_1), any());
    }

    @Test
    public void shouldDeleteWatchInstance_2() {
        when(instancesRepository.delete(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2)).thenReturn(true);

        StandardResponse response = service.deleteWatchInstance(new DeleteWatchInstanceRequest(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2));

        assertThat(response.getStatus(), equalTo(SC_OK));
        verify(notifier).send(eq(TENANT_ID_2), any());
    }

    @Test
    public void shouldDeleteWatchStateAfterWatchInstanceDeletion() {
        when(instancesRepository.delete(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1)).thenReturn(true);
        service.deleteWatchInstance(new DeleteWatchInstanceRequest(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1));
        verify(notifier).send(eq(TENANT_ID_1), runnableCaptor.capture());
        Runnable deleteStateRunnable = runnableCaptor.getValue();

        deleteStateRunnable.run();

        verify(stateRepository).deleteInstanceState(eq(TENANT_ID_1), eq(WATCH_ID_1), eq(ImmutableList.of(INSTANCE_ID_1)), any());
    }

    @Test
    public void shouldNotDeleteWatchInstanceWhichDoesNotExist() {
        StandardResponse response = service.deleteWatchInstance(new DeleteWatchInstanceRequest(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_1));

        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
        verify(instancesRepository).delete(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_1);
    }

    @Test
    public void shouldCreateSingleWatchInstanceWithUsageOfBulkRequest()
        throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1, false);
        when(signalsTenant.watchesExist(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1)).thenReturn(existingWatches);
        when(instancesRepository.findByWatchId(TENANT_ID_1, WATCH_ID_1)).thenReturn(ImmutableList.empty());
        DocNode docNode = DocNode.of(INSTANCE_ID_1, DocNode.of(INSTANCE_PARAM_1, "param-value"));
        StringDoc unparsedDocument = new StringDoc(docNode.toJsonString(), Format.JSON);
        UpsertManyGenericWatchInstancesRequest request =
            new UpsertManyGenericWatchInstancesRequest(TENANT_ID_1, WATCH_ID_1, unparsedDocument);

        StandardResponse response = service.upsertManyInstances(request);

        assertThat(response.getStatus(), equalTo(SC_CREATED));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_1), any(Runnable.class));
        WatchInstanceData watchInstanceData = watchInstanceDataCaptor.getValue();
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_1));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, "param-value"));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(1));
    }

    @Test
    public void shouldCreateManyWatchInstances_1() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_1)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_1)).thenReturn(Optional.of(value));
        String[] fullInstanceIds = new String[]{
            WATCH_ID_1 + "/instances/" + INSTANCE_ID_1,
            WATCH_ID_1 + "/instances/" + INSTANCE_ID_2,
            WATCH_ID_1 + "/instances/" + INSTANCE_ID_3
        };
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_1 + "/instances/" + INSTANCE_ID_1, false,
            WATCH_ID_1 + "/instances/" + INSTANCE_ID_2, false,
            WATCH_ID_1 + "/instances/" + INSTANCE_ID_3, false);
        when(signalsTenant.watchesExist(fullInstanceIds)).thenReturn(existingWatches);
        when(instancesRepository.findByWatchId(TENANT_ID_1, WATCH_ID_1)).thenReturn(ImmutableList.empty());
        DocNode docNode = DocNode.of(INSTANCE_ID_1, DocNode.of(INSTANCE_PARAM_1, "param-value"),
            INSTANCE_ID_2, DocNode.of(INSTANCE_PARAM_1, 2),
            INSTANCE_ID_3, DocNode.of(INSTANCE_PARAM_1, false));
        StringDoc unparsedDocument = new StringDoc(docNode.toJsonString(), Format.JSON);
        UpsertManyGenericWatchInstancesRequest request =
            new UpsertManyGenericWatchInstancesRequest(TENANT_ID_1, WATCH_ID_1, unparsedDocument);

        StandardResponse response = service.upsertManyInstances(request);

        assertThat(response.getStatus(), equalTo(SC_CREATED));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_1), any(Runnable.class));
        List<WatchInstanceData> storedInstanceData = watchInstanceDataCaptor.getAllValues();
        assertThat(storedInstanceData, hasSize(3));
        WatchInstanceData watchInstanceData = storedInstanceData.get(0);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_1));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, "param-value"));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(1));
        watchInstanceData = storedInstanceData.get(1);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_2));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, 2));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(1));
        watchInstanceData = storedInstanceData.get(2);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_3));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, false));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(1));
    }

    @Test
    public void shouldCreateManyWatchInstances_2() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_2)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_2)).thenReturn(Optional.of(value));
        String[] fullInstanceIds = new String[]{
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_1,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2,
        };
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_2 + "/instances/" + INSTANCE_ID_1, false,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2, false);
        when(signalsTenant.watchesExist(fullInstanceIds)).thenReturn(existingWatches);
        when(instancesRepository.findByWatchId(TENANT_ID_2, WATCH_ID_2)).thenReturn(ImmutableList.empty());
        DocNode docNode = DocNode.of(INSTANCE_ID_1, DocNode.of(INSTANCE_PARAM_1, Math.PI, INSTANCE_PARAM_2, 2.0),
            INSTANCE_ID_2, DocNode.of(INSTANCE_PARAM_1, Math.E, INSTANCE_PARAM_2, Long.MIN_VALUE));
        StringDoc unparsedDocument = new StringDoc(docNode.toJsonString(), Format.JSON);
        UpsertManyGenericWatchInstancesRequest request =
            new UpsertManyGenericWatchInstancesRequest(TENANT_ID_2, WATCH_ID_2, unparsedDocument);

        StandardResponse response = service.upsertManyInstances(request);

        assertThat(response.getStatus(), equalTo(SC_CREATED));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_2), any(Runnable.class));
        List<WatchInstanceData> storedInstanceData = watchInstanceDataCaptor.getAllValues();
        assertThat(storedInstanceData, hasSize(2));
        WatchInstanceData watchInstanceData = storedInstanceData.get(0);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_1));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, Math.PI));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, 2.0));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(2));
        watchInstanceData = storedInstanceData.get(1);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_2));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, Math.E));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, Long.MIN_VALUE));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(2));
    }

    @Test
    public void shouldPerformValidationDuringCreationOfManyWatchInstances() throws ConfigValidationException, NoSuchTenantException, SignalsUnavailableException {
        when(signals.getTenant(TENANT_ID_2)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_2)).thenReturn(Optional.of(value));
        String[] fullInstanceIds = new String[]{
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_1,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2,
        };
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_2 + "/instances/" + INSTANCE_ID_1, false,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2, false);
        when(signalsTenant.watchesExist(fullInstanceIds)).thenReturn(existingWatches);
        DocNode docNode = DocNode.of(INSTANCE_ID_1, DocNode.of(INSTANCE_PARAM_1, Math.PI),
            INSTANCE_ID_2, DocNode.of(INSTANCE_PARAM_1, Math.E, INSTANCE_PARAM_2, Long.MIN_VALUE, "incorrect", "parameter"));
        StringDoc unparsedDocument = new StringDoc(docNode.toJsonString(), Format.JSON);
        UpsertManyGenericWatchInstancesRequest request =
            new UpsertManyGenericWatchInstancesRequest(TENANT_ID_2, WATCH_ID_2, unparsedDocument);

        ConfigValidationException ex = (ConfigValidationException)
            assertThatThrown(() -> service.upsertManyInstances(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(2));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$.instance_id_1[0].error", "Watch instance does not contain required parameters: ['param_2']"));
        assertThat(errors, containSubstring("$.instance_id_2[0].error", "Incorrect parameter names: ['incorrect']"));
        verify(notifier, never()).send(eq(TENANT_ID_2), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldUpdateWatchInstancesViaBulkRequest()
        throws NoSuchTenantException, SignalsUnavailableException, ConfigValidationException {
        when(signals.getTenant(TENANT_ID_2)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_2)).thenReturn(Optional.of(value));
        String[] fullInstanceIds = new String[]{
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_1,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2,
        };
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_2 + "/instances/" + INSTANCE_ID_1, false,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2, false);
        when(signalsTenant.watchesExist(fullInstanceIds)).thenReturn(existingWatches);
        ImmutableMap<String, Object> paramsValues = ImmutableMap.of(INSTANCE_PARAM_1, "old value 1", INSTANCE_PARAM_2, "old value 2");
        WatchInstanceData existingWatchToBeUpdated = new WatchInstanceData(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, true, paramsValues);
        when(instancesRepository.findByWatchId(TENANT_ID_2, WATCH_ID_2)).thenReturn(ImmutableList.of(existingWatchToBeUpdated));
        DocNode docNode = DocNode.of(INSTANCE_ID_1, DocNode.of(INSTANCE_PARAM_1, Math.PI, INSTANCE_PARAM_2, 2.0),
            INSTANCE_ID_2, DocNode.of(INSTANCE_PARAM_1, Math.E, INSTANCE_PARAM_2, Long.MIN_VALUE));
        StringDoc unparsedDocument = new StringDoc(docNode.toJsonString(), Format.JSON);
        UpsertManyGenericWatchInstancesRequest request =
            new UpsertManyGenericWatchInstancesRequest(TENANT_ID_2, WATCH_ID_2, unparsedDocument);

        StandardResponse response = service.upsertManyInstances(request);

        assertThat(response.getStatus(), equalTo(SC_OK));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).store(watchInstanceDataCaptor.capture());
        inOrder.verify(notifier).send(eq(TENANT_ID_2), any(Runnable.class));
        List<WatchInstanceData> storedInstanceData = watchInstanceDataCaptor.getAllValues();
        assertThat(storedInstanceData, hasSize(2));
        WatchInstanceData watchInstanceData = storedInstanceData.get(0);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_1));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, Math.PI));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, 2.0));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(2));
        watchInstanceData = storedInstanceData.get(1);
        assertThat(watchInstanceData.getInstanceId(), equalTo(INSTANCE_ID_2));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_1, Math.E));
        assertThat(watchInstanceData.getParameters(), hasEntry(INSTANCE_PARAM_2, Long.MIN_VALUE));
        assertThat(watchInstanceData.getParameters(), aMapWithSize(2));
    }

    @Test
    public void shouldDetectValidationErrorsDuringBatchUpdate()
        throws NoSuchTenantException, SignalsUnavailableException, ConfigValidationException {
        when(signals.getTenant(TENANT_ID_2)).thenReturn(signalsTenant);
        Instances value = new Instances(true, ImmutableList.of(INSTANCE_PARAM_1, INSTANCE_PARAM_2));
        when(signalsTenant.findGenericWatchInstanceConfig(WATCH_ID_2)).thenReturn(Optional.of(value));
        String[] fullInstanceIds = new String[]{
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_1,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2,
        };
        ImmutableMap<String, Boolean> existingWatches = ImmutableMap.of(WATCH_ID_2 + "/instances/" + INSTANCE_ID_1, false,
            WATCH_ID_2 + "/instances/" + INSTANCE_ID_2, false);
        when(signalsTenant.watchesExist(fullInstanceIds)).thenReturn(existingWatches);
        DocNode docNode = DocNode.of(INSTANCE_ID_1, DocNode.of(INSTANCE_PARAM_1, Math.PI, INSTANCE_PARAM_2, 2.0),
            INSTANCE_ID_2, DocNode.of(INSTANCE_PARAM_1, Math.E, INSTANCE_PARAM_2, Long.MIN_VALUE, "Additional param", "cause error"));
        StringDoc unparsedDocument = new StringDoc(docNode.toJsonString(), Format.JSON);
        UpsertManyGenericWatchInstancesRequest request =
            new UpsertManyGenericWatchInstancesRequest(TENANT_ID_2, WATCH_ID_2, unparsedDocument);

        ConfigValidationException ex = (ConfigValidationException)
            assertThatThrown(() -> service.upsertManyInstances(request), instanceOf(ConfigValidationException.class));

        assertThat(ex.toMap(), aMapWithSize(1));
        DocNode errors = DocNode.wrap(ex.toMap());
        assertThat(errors, containSubstring("$.instance_id_2[0].error", "Incorrect parameter names: ['Additional param']"));
        verify(notifier, never()).send(eq(TENANT_ID_2), any(Runnable.class));
        verify(instancesRepository, never()).store(any(WatchInstanceData.class));
    }

    @Test
    public void shouldLoadExistingWatchInstances_1() throws DocumentParseException {
        WatchInstanceData firstInstance = new WatchInstanceData(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_1, true, ImmutableMap.of(INSTANCE_PARAM_1, 1));
        WatchInstanceData secondInstance = new WatchInstanceData(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_2, true, ImmutableMap.of(INSTANCE_PARAM_1, 2));
        when(instancesRepository.findByWatchId(TENANT_ID_1, WATCH_ID_2)).thenReturn(ImmutableList.of(firstInstance, secondInstance));

        StandardResponse response = service.findAllInstances(new GetAllWatchInstancesRequest(TENANT_ID_1, WATCH_ID_2));

        assertThat(response.getStatus(), equalTo(SC_OK));
        DocNode responseBody = responseToDocNode(response);
        assertThat(responseBody, containsValue("$.data.instance_id_1.param_1", 1));
        assertThat(responseBody, containsValue("$.data.instance_id_2.param_1", 2));
    }

    @Test
    public void shouldLoadExistingWatchInstances_2() throws DocumentParseException {
        WatchInstanceData firstInstance = new WatchInstanceData(TENANT_ID_2, WATCH_ID_1, INSTANCE_ID_3, true, ImmutableMap.of(INSTANCE_PARAM_3, 3.33));
        when(instancesRepository.findByWatchId(TENANT_ID_2, WATCH_ID_1)).thenReturn(ImmutableList.of(firstInstance));

        StandardResponse response = service.findAllInstances(new GetAllWatchInstancesRequest(TENANT_ID_2, WATCH_ID_1));

        assertThat(response.getStatus(), equalTo(SC_OK));
        DocNode responseBody = responseToDocNode(response);
        assertThat(responseBody, containsValue("$.data.instance_id_3.param_3", 3.33));
    }

    private static DocNode responseToDocNode(StandardResponse response) throws DocumentParseException {
        return DocNode.parse(Format.JSON).from(DocNode.wrap(response).toJsonString());
    }

    @Test
    public void shouldReturnNotFoundResponseWhenWatchHasNoInstancesDefined() {
        when(instancesRepository.findByWatchId(TENANT_ID_1, WATCH_ID_2)).thenReturn(ImmutableList.empty());

        StandardResponse response = service.findAllInstances(new GetAllWatchInstancesRequest(TENANT_ID_1, WATCH_ID_2));

        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
    }

    @Test
    public void shouldGetWatchParameters_1() throws DocumentParseException {
        ImmutableMap<String, Object> parameters = ImmutableMap.of(INSTANCE_PARAM_1, 1492, INSTANCE_PARAM_2, 112);
        WatchInstanceData watchInstanceData = new WatchInstanceData(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, true, parameters);
        when(instancesRepository.findOneById(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1)).thenReturn(Optional.of(watchInstanceData));
        GetWatchInstanceParametersRequest request = new GetWatchInstanceParametersRequest(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1);

        StandardResponse response = service.getWatchInstanceParameters(request);

        assertThat(response.getStatus(), equalTo(SC_OK));
        DocNode docNode = responseToDocNode(response);
        assertThat(docNode, containsValue("$.data.param_1", 1492));
        assertThat(docNode, containsValue("$.data.param_2", 112));
    }

    @Test
    public void shouldGetWatchParameters_2() throws DocumentParseException {
        ImmutableMap<String, Object> parameters = ImmutableMap.of(INSTANCE_PARAM_3, 1492.5f, INSTANCE_PARAM_4, 112.211);
        WatchInstanceData watchInstanceData = new WatchInstanceData(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, true, parameters);
        when(instancesRepository.findOneById(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2)).thenReturn(Optional.of(watchInstanceData));
        GetWatchInstanceParametersRequest request = new GetWatchInstanceParametersRequest(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2);

        StandardResponse response = service.getWatchInstanceParameters(request);

        assertThat(response.getStatus(), equalTo(SC_OK));
        DocNode docNode = responseToDocNode(response);
        assertThat(docNode, containsValue("$.data.param_3", 1492.5f));
        assertThat(docNode, containsValue("$.data.param_4", 112.211));
    }

    @Test
    public void shouldReturnNotFoundResponseWhenGettingWatchParameters() {
        when(instancesRepository.findOneById(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2)).thenReturn(Optional.empty());
        GetWatchInstanceParametersRequest request = new GetWatchInstanceParametersRequest(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2);

        StandardResponse response = service.getWatchInstanceParameters(request);

        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
    }

    @Test
    public void shouldEnableWatchInstance_1() {
        when(instancesRepository.updateEnabledFlag(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, true)).thenReturn(true);

        StandardResponse response = service.switchEnabledFlag(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, true);

        assertThat(response.getStatus(), equalTo(SC_OK));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).updateEnabledFlag(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, true);
        inOrder.verify(notifier).send(eq(TENANT_ID_1), any(Runnable.class));
        verifyNoMoreInteractions(instancesRepository);
    }

    @Test
    public void shouldEnableWatchInstance_2() {
        when(instancesRepository.updateEnabledFlag(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, true)).thenReturn(true);

        StandardResponse response = service.switchEnabledFlag(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, true);

        assertThat(response.getStatus(), equalTo(SC_OK));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).updateEnabledFlag(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, true);
        inOrder.verify(notifier).send(eq(TENANT_ID_2), any(Runnable.class));
        verifyNoMoreInteractions(instancesRepository);
    }

    @Test
    public void shouldNotEnableNonExistingWatchInstance() {
        when(instancesRepository.updateEnabledFlag(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_1, true)).thenReturn(false);

        StandardResponse response = service.switchEnabledFlag(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_1, true);

        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository).updateEnabledFlag(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_1, true);
        verifyNoMoreInteractions(instancesRepository);
    }

    @Test
    public void shouldDisableWatchInstance_1() {
        when(instancesRepository.updateEnabledFlag(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, false)).thenReturn(true);

        StandardResponse response = service.switchEnabledFlag(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, false);

        assertThat(response.getStatus(), equalTo(SC_OK));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).updateEnabledFlag(TENANT_ID_1, WATCH_ID_1, INSTANCE_ID_1, false);
        inOrder.verify(notifier).send(eq(TENANT_ID_1), any(Runnable.class));
        verifyNoMoreInteractions(instancesRepository);
    }

    @Test
    public void shouldDisableWatchInstance_2() {
        when(instancesRepository.updateEnabledFlag(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, false)).thenReturn(true);

        StandardResponse response = service.switchEnabledFlag(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, false);

        assertThat(response.getStatus(), equalTo(SC_OK));
        InOrder inOrder = inOrder(instancesRepository, notifier);
        inOrder.verify(instancesRepository).updateEnabledFlag(TENANT_ID_2, WATCH_ID_2, INSTANCE_ID_2, false);
        inOrder.verify(notifier).send(eq(TENANT_ID_2), any(Runnable.class));
        verifyNoMoreInteractions(instancesRepository);
    }

    @Test
    public void shouldNotDisableNonExistingWatchInstance() {
        when(instancesRepository.updateEnabledFlag(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_2, false)).thenReturn(false);

        StandardResponse response = service.switchEnabledFlag(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_2, false);

        assertThat(response.getStatus(), equalTo(SC_NOT_FOUND));
        verify(notifier, never()).send(eq(TENANT_ID_1), any(Runnable.class));
        verify(instancesRepository).updateEnabledFlag(TENANT_ID_1, WATCH_ID_2, INSTANCE_ID_2, false);
        verifyNoMoreInteractions(instancesRepository);
    }

    @Test
    public void shouldDeleteInstancesWithState_1() {
        ImmutableList<String> instancesIds = ImmutableList.of(INSTANCE_ID_1, INSTANCE_ID_2, INSTANCE_ID_3);
        when(instancesRepository.deleteByWatchId(TENANT_ID_1, WATCH_ID_1)).thenReturn(instancesIds);

        service.deleteAllInstanceParametersWithState(TENANT_ID_1, WATCH_ID_1);

        verify(stateRepository).deleteInstanceState(eq(TENANT_ID_1), eq(WATCH_ID_1), same(instancesIds), any());
    }

    @Test
    public void shouldDeleteInstancesWithState_2() {
        ImmutableList<String> instancesIds = ImmutableList.of(INSTANCE_ID_3);
        when(instancesRepository.deleteByWatchId(TENANT_ID_2, WATCH_ID_2)).thenReturn(instancesIds);

        service.deleteAllInstanceParametersWithState(TENANT_ID_2, WATCH_ID_2);

        verify(stateRepository).deleteInstanceState(eq(TENANT_ID_2), eq(WATCH_ID_2), same(instancesIds), any());
    }

    @Test
    public void shouldNotDeleteStateWhenInstancesDoNotExists() {
        when(instancesRepository.deleteByWatchId(TENANT_ID_1, WATCH_ID_2)).thenReturn(ImmutableList.empty());

        service.deleteAllInstanceParametersWithState(TENANT_ID_1, WATCH_ID_2);

        Mockito.verifyZeroInteractions(stateRepository);
    }
}