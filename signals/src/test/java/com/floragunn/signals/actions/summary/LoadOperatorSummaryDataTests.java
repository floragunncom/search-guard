package com.floragunn.signals.actions.summary;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchsupport.junit.matcher.DocNodeMatchers;
import com.floragunn.signals.actions.summary.LoadOperatorSummaryData.ActionSummary;
import com.floragunn.signals.actions.summary.LoadOperatorSummaryData.WatchSummary;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.floragunn.searchsupport.junit.matcher.DocNodeMatchers.containsValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LoadOperatorSummaryDataTests {

    public static final Instant NOW = Instant.parse("2024-06-01T12:00:00Z");

    private static Map<String, Object> extractActionsFromWatchNumber(LoadOperatorSummaryData filtered, int index) {
        Map<String, Object> obj = filtered.toBasicObject();
        List<Map<String, Object>> watches = extractWatchesMap(obj);
        return (Map<String, Object>) watches.get(index).get(WatchSummary.FIELD_ACTIONS);
    }

    private static List<Map<String, Object>> extractWatchesMap(Map<String, Object> obj) {
        return (List<Map<String, Object>>) obj.get(LoadOperatorSummaryData.FIELD_WATCHES);
    }

    private ActionSummary actionSummary() {
        return new ActionSummary(NOW, NOW, true, NOW, "err", "200", "ok");
    }

    private WatchSummary watchSummary(String id, Map<String, ActionSummary> actions) {
        return new WatchSummary(id, "status", "sev", "desc", null, actions, "reason");
    }

    @Test
    public void shouldFilterActionsByAllowedNames() {
        Map<String, ActionSummary> actions = new HashMap<>();
        actions.put("existing_action", actionSummary());
        actions.put("deleted_action", actionSummary());
        WatchSummary watchSummary = watchSummary("tenant1/watch1", actions);
        LoadOperatorSummaryData summaryData = new LoadOperatorSummaryData(List.of(watchSummary));
        WatchActionNames watchActionNames = new WatchActionNames("tenant1/watch1", List.of("existing_action"));

        LoadOperatorSummaryData filteredSummaryData = summaryData.filterActions(List.of(watchActionNames));

        Map<String, Object> actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 0);
        assertThat(actionsMap, aMapWithSize(1));
        assertThat(actionsMap.keySet(), contains("existing_action"));
    }

    @Test
    public void shouldRemoveAllActionsIfWatchDoesNotContainsAnyAction() {
        Map<String, ActionSummary> actions = new HashMap<>();
        actions.put("existing_action", actionSummary());
        actions.put("deleted_action", actionSummary());
        WatchSummary watchSummary = watchSummary("tenant1/watch1", actions);
        LoadOperatorSummaryData summaryData = new LoadOperatorSummaryData(List.of(watchSummary));
        WatchActionNames watchActionNames = new WatchActionNames("tenant1/watch1", List.of());

        LoadOperatorSummaryData filteredSummaryData = summaryData.filterActions(List.of(watchActionNames));

        Map<String, Object> actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 0);
        assertThat(actionsMap, aMapWithSize(0));
    }

    @Test
    public void shouldFilterActionsByMultipleAllowedNames() {
        Map<String, ActionSummary> actions = new HashMap<>();
        actions.put("existing_action_1", actionSummary());
        actions.put("existing_action_2", actionSummary());
        actions.put("existing_action_3", actionSummary());
        actions.put("deleted_action_1", actionSummary());
        actions.put("deleted_action_2", actionSummary());
        actions.put("deleted_action_3", actionSummary());
        WatchSummary watchSummary_1 = watchSummary("tenant1/watch1", new HashMap<>(actions));
        WatchSummary watchSummary_2 = watchSummary("tenant1/watch2", new HashMap<>(actions));
        LoadOperatorSummaryData summaryData = new LoadOperatorSummaryData(List.of(watchSummary_1, watchSummary_2));
        WatchActionNames watchActionNames_1 = new WatchActionNames("tenant1/watch1", List.of("existing_action_1"));
        WatchActionNames watchActionNames_2 = new WatchActionNames("tenant1/watch2", List.of("existing_action_2", "existing_action_3"));

        LoadOperatorSummaryData filteredSummaryData = summaryData.filterActions(List.of(watchActionNames_1, watchActionNames_2));

        Map<String, Object> actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 0);
        assertThat(actionsMap, aMapWithSize(1));
        assertThat(actionsMap.keySet(), contains("existing_action_1"));
        actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 1);
        assertThat(actionsMap, aMapWithSize(2));
        assertThat(actionsMap.keySet(), containsInAnyOrder("existing_action_2", "existing_action_3"));
    }

    @Test
    public void shouldReturnSameDataIfAllowedNamesListIsEmpty() {
        // Watch with such id was not find however watch state exist, very edge case.
        Map<String, ActionSummary> actions = new HashMap<>();
        actions.put("action_1", actionSummary());
        WatchSummary ws = watchSummary("tenant/watch", actions);
        LoadOperatorSummaryData data = new LoadOperatorSummaryData(List.of(ws));

        LoadOperatorSummaryData filteredSummaryData = data.filterActions(Collections.emptyList());// do not perform filtering, because ac

        Map<String, Object> actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 0);
        assertThat(actionsMap.keySet(), contains("action_1"));
    }

    @Test
    public void shouldHandleWatchWithNoMatchingActionNames() {
        //Watch does not contain any action in its definition, but some actions are present in state
        Map<String, ActionSummary> actions = new HashMap<>();
        actions.put("action_1", actionSummary());
        WatchSummary watchSummary = watchSummary("tenant/watch", actions);
        LoadOperatorSummaryData summaryData = new LoadOperatorSummaryData(List.of(watchSummary));
        WatchActionNames watchActionNames = new WatchActionNames("tenant/watch", List.of("not_present"));

        LoadOperatorSummaryData filteredSummaryData = summaryData.filterActions(
                List.of(watchActionNames));// just remove all actions because non actions are present in watch definition

        Map<String, Object> actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 0);
        assertThat(actionsMap.keySet(), empty());
    }

    @Test
    public void shouldNotFailIfWatchIdNotInAllowedList() {
        // Handling of watches not present in allowed list (should not filter, very edge case)
        Map<String, ActionSummary> actions = new HashMap<>();
        actions.put("action_1", actionSummary());
        WatchSummary watchSummary = watchSummary("tenant/watch", actions);
        LoadOperatorSummaryData data = new LoadOperatorSummaryData(List.of(watchSummary));
        WatchActionNames watchActionNames = new WatchActionNames("tenant/other_watch",
                List.of("another_action")); // watch "tenant/other_watch" does not exist in state index

        LoadOperatorSummaryData filteredSummaryData = data.filterActions(List.of(watchActionNames));

        Map<String, Object> actionsMap = extractActionsFromWatchNumber(filteredSummaryData, 0);
        assertThat(actionsMap.keySet(), contains("action_1")); // omit filtering
    }

    @Test
    public void shouldCombineUniqueWatchSummaries() {
        Map<String, ActionSummary> actions1 = new HashMap<>();
        actions1.put("action_1", actionSummary());
        Map<String, ActionSummary> actions2 = new HashMap<>();
        actions2.put("action_2", actionSummary());
        WatchSummary watchSummary_1 = watchSummary("tenant/watch_1", actions1);
        WatchSummary watchSummary_2 = watchSummary("tenant/watch_2", actions2);
        LoadOperatorSummaryData summaryData_1 = new LoadOperatorSummaryData(List.of(watchSummary_1));
        LoadOperatorSummaryData summaryData_2 = new LoadOperatorSummaryData(List.of(watchSummary_1, watchSummary_2));

        LoadOperatorSummaryData combined = summaryData_1.with(summaryData_2);

        DocNode watches = DocNode.wrap(combined.toBasicObject());
        assertThat(watches.getAsNode("watches").size(), equalTo(2));
        assertThat(watches, containsValue("watches[0].watch_id", "watch_1"));
        assertThat(watches, DocNodeMatchers.docNodeSizeEqualTo("watches[0].actions", 1));
        assertThat(watches, containsValue("watches[0].actions.action_1.check_result", true));
        assertThat(watches, containsValue("watches[1].watch_id", "watch_2"));
        assertThat(watches, DocNodeMatchers.docNodeSizeEqualTo("watches[1].actions", 1));
        assertThat(watches, containsValue("watches[1].actions.action_2.check_result", true));
    }

    @Test
    public void shouldNotAddDuplicatesWhenCombining() {
        // Exposes: Duplicate detection logic
        WatchSummary watchSummary = watchSummary("tenant/watch", Map.of());
        LoadOperatorSummaryData summaryData_1 = new LoadOperatorSummaryData(List.of(watchSummary));
        LoadOperatorSummaryData summaryData_2 = new LoadOperatorSummaryData(List.of(watchSummary));

        LoadOperatorSummaryData combined = summaryData_1.with(summaryData_2);

        Map<String, Object> obj = combined.toBasicObject();
        List<Map<String, Object>> watches = extractWatchesMap(obj);
        assertThat(watches, hasSize(1));
    }

    @Test
    public void shouldCombineMultipleWatches() {
        WatchSummary watchSummary_1 = watchSummary("tenant/watch_1", Map.of());
        WatchSummary watchSummary_2 = watchSummary("tenant/watch_2", Map.of());
        WatchSummary watchSummary_3 = watchSummary("tenant/watch_3", Map.of());
        WatchSummary watchSummary_4 = watchSummary("tenant/watch_4", Map.of());
        WatchSummary watchSummary_5 = watchSummary("tenant/watch_5", Map.of());
        WatchSummary watchSummary_6 = watchSummary("tenant/watch_6", Map.of());
        LoadOperatorSummaryData summaryData_1 = new LoadOperatorSummaryData(List.of(watchSummary_1, watchSummary_2, watchSummary_3, watchSummary_4));
        LoadOperatorSummaryData summaryData_2 = new LoadOperatorSummaryData(
                List.of(watchSummary_2, watchSummary_3, watchSummary_4, watchSummary_5, watchSummary_6));

        LoadOperatorSummaryData combined = summaryData_1.with(summaryData_2);

        Map<String, Object> obj = combined.toBasicObject();
        List<Map<String, Object>> watches = extractWatchesMap(obj);
        assertThat(watches, hasSize(6));
        List<String> watchIds = watches.stream() //
                .map(map -> map.get("watch_id")) //
                .map(String.class::cast) //
                .toList();
        assertThat(watchIds, containsInAnyOrder("watch_1", "watch_2", "watch_3", "watch_4", "watch_5", "watch_6"));
    }
}

