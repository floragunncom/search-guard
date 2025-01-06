package com.floragunn.aim;

import com.floragunn.aim.api.internal.InternalPolicyAPI;
import com.floragunn.aim.api.internal.InternalPolicyInstanceAPI;
import com.floragunn.aim.api.internal.InternalSettingsAPI;
import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.actions.Action;
import com.floragunn.aim.policy.actions.AllocationAction;
import com.floragunn.aim.policy.actions.CloseAction;
import com.floragunn.aim.policy.actions.DeleteAction;
import com.floragunn.aim.policy.actions.ForceMergeAsyncAction;
import com.floragunn.aim.policy.actions.RolloverAction;
import com.floragunn.aim.policy.actions.SetPriorityAction;
import com.floragunn.aim.policy.actions.SetReadOnlyAction;
import com.floragunn.aim.policy.actions.SetReplicaCountAction;
import com.floragunn.aim.policy.actions.SnapshotAsyncAction;
import com.floragunn.aim.policy.conditions.AgeCondition;
import com.floragunn.aim.policy.conditions.Condition;
import com.floragunn.aim.policy.conditions.DocCountCondition;
import com.floragunn.aim.policy.conditions.ForceMergeDoneCondition;
import com.floragunn.aim.policy.conditions.IndexCountCondition;
import com.floragunn.aim.policy.conditions.SizeCondition;
import com.floragunn.aim.policy.conditions.SnapshotCreatedCondition;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.policy.schedule.IntervalSchedule;
import com.floragunn.aim.policy.schedule.Schedule;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Format;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("UnitTest")
@Execution(ExecutionMode.CONCURRENT)
public class ParsingTest {
    private static Stream<Schedule> scheduleStream() {
        return ImmutableList.of((Schedule) new IntervalSchedule(Duration.ofMinutes(10), false, Schedule.Scope.DEFAULT)).stream();
    }

    private static Stream<Action> actionStream() {
        return ImmutableList.of(new AllocationAction(ImmutableMap.of("box_type", "warm"), ImmutableMap.empty(), ImmutableMap.empty()),
                new CloseAction(), new ForceMergeAsyncAction(2), new RolloverAction(), new SetPriorityAction(50), new SetReadOnlyAction(),
                new SetReplicaCountAction(2), new SnapshotAsyncAction("test_snapshot", "test_repo")).stream();
    }

    private static Stream<Condition> conditionStream() {
        return ImmutableList.of(new ForceMergeDoneCondition(2), new AgeCondition(TimeValue.timeValueSeconds(3)), new DocCountCondition(0),
                new IndexCountCondition("test_alias_key", 50), new SizeCondition(ByteSizeValue.ofBytes(5)), new SnapshotCreatedCondition("test_repo"))
                .stream();
    }

    private static Stream<Policy.Step> stepStream() {
        return ImmutableList
                .of(new Policy.Step("first", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))), ImmutableList.of(new DeleteAction())))
                .stream();
    }

    private static Stream<Policy> policyStream() {
        return ImmutableList.of(new Policy(
                new Policy.Step("first", ImmutableList.of(new SizeCondition(ByteSizeValue.ofGb(4))), ImmutableList.of(new RolloverAction())),
                new Policy.Step("second", ImmutableList.of(new AgeCondition(TimeValue.timeValueDays(30))), ImmutableList.of(new DeleteAction()))))
                .stream();
    }

    private static Stream<? extends TransportRequest> internalRequestStream() {
        ImmutableList<TransportRequest> settings = ImmutableList
                .of(AutomatedIndexManagementSettings.Dynamic.getAvailableSettings().stream().filter(attribute -> attribute.getDefaultValue() != null)
                        .map(attribute -> new InternalSettingsAPI.Update.Request(ImmutableMap.of(attribute, attribute.getDefaultValue()),
                                ImmutableList.of(attribute)))
                        .collect(Collectors.toList()));
        return settings.with(ImmutableList.of(
                new InternalPolicyAPI.Refresh.Request.Node(new InternalPolicyAPI.Refresh.Request(
                        ImmutableList.of("index_1", "index_2"), ImmutableList.of("index_3"), ImmutableList.of("index_4"))),
                new InternalPolicyAPI.Delete.Request("policy_1", true),
                new InternalPolicyAPI.Put.Request("policy_2", new Policy(new Policy.Step("step_1", ImmutableList.empty(), ImmutableList.empty())),
                        false),
                new InternalPolicyInstanceAPI.PostExecuteRetry.Request.Node(
                        new InternalPolicyInstanceAPI.PostExecuteRetry.Request("index_1", true, true))))
                .stream();
    }

    private static Stream<? extends TransportResponse> internalResponseStream() {
        return ImmutableList.of(new InternalSettingsAPI.Refresh.Response(ClusterName.DEFAULT, new ArrayList<>(), new ArrayList<>()),
                new InternalSettingsAPI.Update.Response(ImmutableList.of(AutomatedIndexManagementSettings.Dynamic.getAvailableSettings()), false))
                .stream();
    }

    private static Stream<PolicyInstanceState> policyInstanceStateStream() {
        return ImmutableList.of(new PolicyInstanceState("policy"),
                new PolicyInstanceState("policy").setStatus(PolicyInstanceState.Status.DELETED)
                        .setLastExecutedStepState(new PolicyInstanceState.StepState("delete", Instant.now(), 0, null))
                        .setLastExecutedActionState(new PolicyInstanceState.ActionState("delete", Instant.now(), 1, null)),
                new PolicyInstanceState("policy").setStatus(PolicyInstanceState.Status.RUNNING)
                        .addCreatedSnapshotName("snapshot_key", "index-2-465245435").setCurrentStepName("current-step")
                        .setLastExecutedConditionState(new PolicyInstanceState.ConditionState("size", Instant.now(), null, null))
                        .setLastExecutedStepState(new PolicyInstanceState.StepState("current-step", Instant.now(), 1,
                                new PolicyInstance.ExecutionException("Condition failed")))
                        .setLastExecutedActionState(new PolicyInstanceState.ActionState("priority", Instant.now(), 1, null)))
                .stream();
    }

    @ParameterizedTest
    @MethodSource("scheduleStream")
    public void testScheduleParsing(Schedule schedule) throws Exception {
        testDocNodeParsing(schedule,
                docNode -> Policy.ParsingContext
                        .lenient(Schedule.Factory.defaultFactory(), Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())
                        .parseSchedule(docNode, schedule.getScope()));
        testJSONStringParsing(schedule,
                docNode -> Policy.ParsingContext
                        .lenient(Schedule.Factory.defaultFactory(), Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())
                        .parseSchedule(docNode, schedule.getScope()));
    }

    @ParameterizedTest
    @MethodSource("actionStream")
    public void testActionParsing(Action action) throws Exception {
        testDocNodeParsing(action,
                docNode -> Policy.ParsingContext
                        .lenient(Schedule.Factory.defaultFactory(), Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())
                        .parseAction(docNode));
        testJSONStringParsing(action,
                docNode -> Policy.ParsingContext
                        .lenient(Schedule.Factory.defaultFactory(), Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())
                        .parseAction(docNode));
    }

    @ParameterizedTest
    @MethodSource("conditionStream")
    public void testConditionParsing(Condition condition) throws Exception {
        testDocNodeParsing(condition,
                docNode -> Policy.ParsingContext
                        .lenient(Schedule.Factory.defaultFactory(), Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())
                        .parseCondition(docNode));
        testJSONStringParsing(condition,
                docNode -> Policy.ParsingContext
                        .lenient(Schedule.Factory.defaultFactory(), Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())
                        .parseCondition(docNode));
    }

    @ParameterizedTest
    @MethodSource("stepStream")
    public void testStepParsing(Policy.Step step) throws Exception {
        testDocNodeParsing(step, docNode -> Policy.Step.parse(docNode, Policy.ParsingContext.lenient(Schedule.Factory.defaultFactory(),
                Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())));
        testJSONStringParsing(step, docNode -> Policy.Step.parse(docNode, Policy.ParsingContext.lenient(Schedule.Factory.defaultFactory(),
                Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())));
    }

    @ParameterizedTest
    @MethodSource("policyStream")
    public void testPolicyParsing(Policy policy) throws Exception {
        testDocNodeParsing(policy, docNode -> Policy.parse(docNode, Policy.ParsingContext.lenient(Schedule.Factory.defaultFactory(),
                Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())));
        testJSONStringParsing(policy, docNode -> Policy.parse(docNode, Policy.ParsingContext.lenient(Schedule.Factory.defaultFactory(),
                Condition.Factory.defaultFactory(), Action.Factory.defaultFactory())));
    }

    @ParameterizedTest
    @MethodSource("internalRequestStream")
    public <T extends TransportRequest> void testInternalRequestParsing(T request) throws Exception {
        testWriteableParsing(request);
    }

    @ParameterizedTest
    @MethodSource("internalResponseStream")
    public <T extends TransportResponse> void testInternalResponseParsing(T response) throws Exception {
        testWriteableParsing(response);
    }

    @ParameterizedTest
    @MethodSource("policyInstanceStateStream")
    public void testPolicyInstanceStateParsing(PolicyInstanceState policyInstanceState) throws Exception {
        testDocNodeParsing(policyInstanceState, PolicyInstanceState::new);
        testJSONStringParsing(policyInstanceState, PolicyInstanceState::new);
    }

    private static <T extends Document<Object>> void testDocNodeParsing(T t, DocNodeParser<T> inputParser) throws Exception {
        DocNode node = t.toDocNode();
        T res = inputParser.parse(node);
        assertEquals(t, res);
    }

    private static <T extends Document<Object>> void testJSONStringParsing(T t, DocNodeParser<T> inputParser) throws Exception {
        String s = t.toJsonString();
        T res = inputParser.parse(DocNode.parse(Format.JSON).from(s));
        assertEquals(t, res);
    }

    private interface DocNodeParser<T> {
        T parse(DocNode docNode) throws Exception;
    }

    private static <T extends ToXContent> void testXContentParsing(T t1) throws Exception {
        testXContentParsing(t1, ToXContent.EMPTY_PARAMS);
    }

    private static <T extends ToXContent> void testXContentParsing(T t1, ToXContent.Params params) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        t1.toXContent(builder, params);
        String t1String = Strings.toString(builder);

        @SuppressWarnings("unchecked")
        Class<T> tClass = (Class<T>) t1.getClass();
        T t2 = tClass.getConstructor(DocNode.class).newInstance(DocNode.parse(Format.JSON).from(t1String));

        builder = XContentFactory.jsonBuilder();
        t2.toXContent(builder, params);
        String t2String = Strings.toString(builder);

        assertEquals(t1String, t2String);
        assertEquals(t1, t2);
    }

    private static <T extends Writeable> void testWriteableParsing(T t1) throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        t1.writeTo(output);

        @SuppressWarnings("unchecked")
        Class<T> tClass = (Class<T>) t1.getClass();
        T t2 = tClass.getConstructor(StreamInput.class).newInstance(output.bytes().streamInput());

        assertEquals(t1, t2);
    }
}
