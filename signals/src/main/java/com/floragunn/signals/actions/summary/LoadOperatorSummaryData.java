package com.floragunn.signals.actions.summary;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;
import com.google.common.base.Strings;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.rest.RestStatus;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getDoubleValue;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getLongValue;

public class LoadOperatorSummaryData implements Document {

    public static final String FIELD_WATCHES = "watches";

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    static class WatchSeverityDetails {
        public static final String FIELD_LEVEL = "level";
        public static final String FIELD_LEVEL_NUMERIC = "level_numeric";
        public static final String FIELD_CURRENT_VALUE = "current_value";
        public static final String FIELD_THRESHOLD = "threshold";
        private final String level;
        private final Long levelNumeric;
        private final Double currentValue;
        private final Double threshold;

        public WatchSeverityDetails(String level, Long levelNumeric, Double currentValue, Double threshold) {
            this.level = level;
            this.levelNumeric = levelNumeric;
            this.currentValue = currentValue;
            this.threshold = threshold;
        }

        public WatchSeverityDetails(DocNode node) {
            this(node.getAsString(FIELD_LEVEL), getLongValue(node, FIELD_LEVEL_NUMERIC), getDoubleValue(node, FIELD_CURRENT_VALUE),
                getDoubleValue(node, FIELD_THRESHOLD));
        }

        Map<String, Object> toBasicObject() {
            return ImmutableMap.of(FIELD_LEVEL, level, FIELD_LEVEL_NUMERIC, levelNumeric, FIELD_CURRENT_VALUE, currentValue,
                FIELD_THRESHOLD, threshold);
        }
    }

    static class ActionSummary {
        public static final String FIELD_TRIGGERED = "triggered";
        public static final String FIELD_CHECKED = "checked";
        public static final String FIELD_CHECK_RESULT = "check_result";
        public static final String FIELD_EXECUTION = "execution";
        public static final String FIELD_ERROR = "error";
        public static final String FIELD_STATUS_CODE = "status_code";
        public static final String FIELD_STATUS_DETAILS = "status_details";
        public static final String FIELD_ACK_BY_USER = "ack_by";
        public static final String FIELD_ACK_ON = "ack_on";
        private final Instant triggered;
        private final Instant checked;
        private final Boolean checkResult;
        private final Instant execution;
        private final String error;
        private final String statusCode;
        private final String statusDetails;
        private final String ackByUser;
        private final Instant ackTime;

        public ActionSummary(
            Instant triggered,
            Instant checked,
            Boolean checkResult,
            Instant execution,
            String error,
            String statusCode,
            String statusDetails,
            String ackByUser,
            Instant ackTime) {
            this.triggered = triggered;
            this.checked = checked;
            this.checkResult = checkResult;
            this.execution = execution;
            this.error = error;
            this.statusCode = statusCode;
            this.statusDetails = statusDetails;
            this.ackByUser = ackByUser;
            this.ackTime = ackTime;
        }

        public static ActionSummary parse(DocNode node) {
            try {
                return new ActionSummary(stringToInstant(node.getAsString(FIELD_TRIGGERED)), stringToInstant(node.getAsString(FIELD_CHECKED)),
                    node.getBoolean(FIELD_CHECK_RESULT), stringToInstant(node.getAsString(FIELD_EXECUTION)), node.getAsString(FIELD_ERROR),
                    node.getAsString(FIELD_STATUS_CODE), node.getAsString(FIELD_STATUS_DETAILS), node.getAsString(FIELD_ACK_BY_USER), stringToInstant(node.getAsString(FIELD_ACK_ON)));
            } catch (ConfigValidationException e) {
                throw new ElasticsearchStatusException("Cannot parse action summary", RestStatus.INTERNAL_SERVER_ERROR, e);
            }
        }

        Map<String, Object> toBasicObject() {
            return ImmutableMap.<String, Object>of(FIELD_TRIGGERED, instantToString(triggered), FIELD_CHECKED, instantToString(checked), FIELD_CHECK_RESULT, checkResult)
                .with(ImmutableMap.of(FIELD_EXECUTION, instantToString(execution), FIELD_ERROR, error, FIELD_STATUS_CODE, statusCode, FIELD_STATUS_DETAILS, statusDetails))
                    .with(ImmutableMap.of(FIELD_ACK_BY_USER, ackByUser, FIELD_ACK_ON, instantToString(ackTime)));
        }

    }

    static class WatchSummary {
        public static final String FIELD_WATCH_ID = "watch_id";
        public static final String FIELD_TENANT = "tenant";
        public static final String FIELD_STATUS_CODE = "status_code";
        public static final String FIELD_SEVERITY = "severity";
        public static final String FIELD_DESCRIPTION = "description";
        public static final String FIELD_SEVERITY_DETAILS = "severity_details";
        public static final String FIELD_ACTIONS = "actions";
        public static final String FIELD_REASON = "reason";
        public static final String FIELD_ACTIVE = "active";
        private final String id;
        private final String statusCode;
        private final String severity;
        private final String description;
        private final WatchSeverityDetails severityDetails;
        private final Boolean active;

        private final Map<String, ActionSummary> actions;
        private final String reason;

        public WatchSummary(String id, String statusCode, String severity, String description, WatchSeverityDetails severityDetails,
            Map<String, ActionSummary> actions, String reason, Boolean active) {
            this.id = id;
            this.statusCode = statusCode;
            this.severity = severity;
            this.description = description;
            this.severityDetails = severityDetails;
            this.actions = Objects.requireNonNull(actions);
            this.reason = Objects.requireNonNull(reason);
            this.active = active;
        }

        private static WatchSummary parse(DocNode node) {
            WatchSeverityDetails details = node.containsKey(FIELD_SEVERITY_DETAILS) ?
                new WatchSeverityDetails(node.getAsNode(FIELD_SEVERITY_DETAILS)) : null;
            DocNode actionsNode = node.getAsNode(FIELD_ACTIONS);
            Map<String, ActionSummary> actionsSummary = new HashMap<>();
            for(String actionName : actionsNode.keySet()) {
                actionsSummary.put(actionName, ActionSummary.parse(actionsNode.getAsNode(actionName)));
            }
            return new WatchSummary(node.getAsString(FIELD_WATCH_ID), node.getAsString(FIELD_STATUS_CODE), node.getAsString(FIELD_SEVERITY),
                node.getAsString(FIELD_DESCRIPTION), details, actionsSummary, node.getAsString(FIELD_REASON), null);
        }

        Map<String, Object> toBasicObject() {
            Map<String, Object> severityMap = Optional.ofNullable(severityDetails)//
                .map(WatchSeverityDetails::toBasicObject)//
                .orElse(null);
            Map<String, Object> actionMap = createActionSummaryMap();
            return ImmutableMap.of(FIELD_WATCH_ID, getPureId(), FIELD_STATUS_CODE, statusCode, FIELD_SEVERITY, severity,
                FIELD_DESCRIPTION, description, FIELD_SEVERITY_DETAILS, severityMap)
                .with(ImmutableMap.of(FIELD_ACTIONS, actionMap, FIELD_TENANT, getTenant(), FIELD_ACTIVE, active, FIELD_REASON, reason));
        }

        private Map<String, Object> createActionSummaryMap() {
            Map<String, Object> actionMap = new HashMap<>();
            for(Map.Entry<String, ActionSummary> entry : actions.entrySet()) {
                actionMap.put(entry.getKey(), entry.getValue().toBasicObject());
            }
            return actionMap;
        }

        public String getPureId() {
            if(Objects.nonNull(id)) {
                int tenantSeparator = id.indexOf("/");
                if(tenantSeparator == -1) {
                    return id;
                }
                return id.substring(tenantSeparator + 1);
            }
            return null;
        }

        public String getTenant() {
            if(Objects.nonNull(id)) {
                int tenantSeparator = id.indexOf("/");
                if(tenantSeparator == -1) {
                    return id;
                }
                return id.substring(0, tenantSeparator);
            }
            return null;
        }

        public WatchSummary filterActions(Set<String> allowedActionNames) {
            if(allowedActionNames == null) {
                return this; // No filtering needed
            }
            Map<String, ActionSummary> newActions = new LinkedHashMap<>();
            for(Map.Entry<String, ActionSummary> entry : actions.entrySet()) {
                if (allowedActionNames.contains(entry.getKey())) {
                    newActions.put(entry.getKey(), entry.getValue());
                }
            }

            return new WatchSummary(this.id, this.statusCode, this.severity, this.description,
                this.severityDetails, newActions, this.reason, this.active);
        }

        WatchSummary withActive(Boolean watchActive) {
            return new WatchSummary(this.id, this.statusCode, this.severity, this.description, this.severityDetails, this.actions, this.reason, watchActive);
        }
    }

    private final List<WatchSummary> watches;

    LoadOperatorSummaryData(UnparsedMessage message) throws ConfigValidationException {
        DocNode docNode = message.requiredDocNode();
        this.watches = docNode.getAsListFromNodes(FIELD_WATCHES, WatchSummary::parse);
    }

    LoadOperatorSummaryData(List<WatchSummary> watches) {
        this.watches = watches;
    }

    public LoadOperatorSummaryData filterActions(List<WatchActionNames> watchActionNames) {
        Map<String, Set<String>> actionNamesByWatchId = watchActionNames.stream()
                .collect(Collectors.toMap(WatchActionNames::watchIdWithTenantPrefix, WatchActionNames::actionNamesAsSet));
        List<WatchSummary> filteredWatches = watches.stream() //
                .map(watchSummary -> watchSummary.filterActions(actionNamesByWatchId.get(watchSummary.id))) //
                .toList();
        return new LoadOperatorSummaryData(filteredWatches);
    }

    public LoadOperatorSummaryData with(LoadOperatorSummaryData loadOperatorSummaryData) {
        List<WatchSummary> newWatches = new ArrayList<>(this.watches);
        Set<String> includedIds = this.watches.stream() //
                .map(summary -> summary.id) //
                .collect(Collectors.toSet());
        for (WatchSummary watch : loadOperatorSummaryData.watches) {
            if (!includedIds.contains(watch.id)) {
                newWatches.add(watch);
            }
        }
        return new LoadOperatorSummaryData(newWatches);
    }

    public LoadOperatorSummaryData withActive(List<WatchActionNames> watchesWithActive) {
        Map<String, Boolean> watchIdToActive = new HashMap<>(watchesWithActive.size());
        for (WatchActionNames watch : watchesWithActive) {
            watchIdToActive.put(watch.watchIdWithTenantPrefix(), watch.active());
        }
        return new LoadOperatorSummaryData(watches.stream() //
                .map(watch -> watch.withActive(watchIdToActive.get(watch.id))) //
                .collect(Collectors.toList()));
    }

    public int getSize() {
        return watches.size();
    }

    @Override
    public Map<String, Object> toBasicObject() {
        List<Map<String, Object>> watchesMap = watches.stream().map(WatchSummary::toBasicObject).collect(Collectors.toList());
        return ImmutableMap.of(FIELD_WATCHES, watchesMap);
    }

    private static String instantToString(Instant instant) {
        return instant == null ? null : DATE_FORMATTER.format(instant);
    }

    private static Instant stringToInstant(String instant) {
        return Strings.isNullOrEmpty(instant) ? null : Instant.from(DATE_FORMATTER.parse(instant));
    }
}
