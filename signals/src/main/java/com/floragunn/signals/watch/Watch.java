package com.floragunn.signals.watch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Trigger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.jobs.config.AbstractJobConfigFactory;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory;
import com.floragunn.searchsupport.jobs.config.schedule.Schedule;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.MissingAttribute;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonParser;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.searchsupport.util.JacksonXContent;
import com.floragunn.searchsupport.util.duration.DurationExpression;
import com.floragunn.signals.execution.WatchRunner;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.action.handlers.AutoResolveActionHandler;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.action.invokers.AutoResolveAction;
import com.floragunn.signals.watch.action.invokers.ResolveAction;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.common.WatchElement;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.severity.SeverityMapping;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;

public class Watch extends WatchElement implements JobConfig, ToXContentObject {
    private final static Logger log = LogManager.getLogger(Watch.class);

    public static Map<String, String> WITHOUT_AUTH_TOKEN_PARAM_MAP = Collections.singletonMap("include_auth_token", "false");

    public static final ToXContent.Params WITHOUT_AUTH_TOKEN = new ToXContent.MapParams(WITHOUT_AUTH_TOKEN_PARAM_MAP);
    public static final ToXContent.Params WITHOUT_META_AND_ACTIVE = new ToXContent.MapParams(
            ImmutableMap.of("include_meta", "false", "include_active", "false"));

    private String tenant;
    private JobKey jobKey;
    private String description;
    private Class<? extends Job> jobClass = WatchRunner.class;
    private Map<String, Object> jobDataMap;
    private Schedule schedule;
    private List<Check> checks;
    private List<AlertAction> actions;
    private List<ResolveAction> resolveActions;
    private Map<String, Object> ui;
    protected DurationExpression throttlePeriod;
    private boolean active = true;
    private boolean logRuntimeData;
    private SeverityMapping severityMapping;
    private Meta meta = new Meta();

    private long version;

    public Watch() {
    }

    public Watch(JobKey jobKey, Schedule schedule, List<Check> checks, SeverityMapping severityMapping, List<AlertAction> actions,
            List<ResolveAction> resolveActions) {
        this.jobKey = jobKey;
        this.schedule = schedule;
        this.checks = checks;
        this.severityMapping = severityMapping;
        this.actions = actions;
        this.resolveActions = resolveActions;
    }

    public String getId() {
        return jobKey.getName();
    }

    @Override
    public JobKey getJobKey() {
        return jobKey;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Class<? extends Job> getJobClass() {
        return jobClass;
    }

    @Override
    public Map<String, Object> getJobDataMap() {
        return jobDataMap;
    }

    @Override
    public boolean isDurable() {
        return true;
    }

    @Override
    public List<Trigger> getTriggers() {
        return schedule.getTriggers();
    }

    public List<Check> getChecks() {
        return checks;
    }

    public List<AlertAction> getActions() {
        return actions;
    }

    public List<ResolveAction> getNonGeneratedResolveActions() {
        if (resolveActions == null || resolveActions.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<ResolveAction> result = new ArrayList<>(resolveActions.size());

        for (ResolveAction resolveAction : this.resolveActions) {
            if (!(resolveAction instanceof AutoResolveAction)) {
                result.add(resolveAction);
            }
        }

        return result;
    }

    public long getVersion() {
        return version;
    }

    public String toJson() {
        return Strings.toString(this);
    }

    public String toJsonWithoutAuthToken() {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {

            this.toXContent(builder, WITHOUT_AUTH_TOKEN);

            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String toJsonWithoutMetaAndActive() {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {

            this.toXContent(builder, WITHOUT_META_AND_ACTIVE);

            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return this.jobKey.getName();
    }

    public int hashCode() {
        return this.jobKey.hashCode();
    }

    public String secureHash() {
        String json = toJsonWithoutMetaAndActive();

        return Hashing.sha256().hashString(json, StandardCharsets.UTF_8).toString();
    }

    public String getIdAndHash() {
        return getId() + "." + secureHash();
    }

    public String getSecureAuthTokenAudience() {
        return getIdAndHash();
    }

    private void initAutoResolveActions() {
        if (this.actions == null || this.actions.size() == 0) {
            return;
        }

        List<ResolveAction> newResolveActions = new ArrayList<>();

        if (this.resolveActions != null) {
            newResolveActions.addAll(this.resolveActions);
        }

        for (AlertAction action : this.actions) {
            if (action.getHandler() instanceof AutoResolveActionHandler && ((AutoResolveActionHandler) action.getHandler()).isAutoResolveEnabled()) {
                newResolveActions.add(new AutoResolveAction(action, ((AutoResolveActionHandler) action.getHandler()).getResolveActionHandler()));
            }
        }

        this.resolveActions = newResolveActions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (tenant != null) {
            builder.field("_tenant", tenant);
        }

        builder.field("trigger");
        builder.startObject();

        builder.field("schedule", schedule);

        builder.endObject();

        if (throttlePeriod != null) {
            builder.field("throttle_period", throttlePeriod.toString());
        }

        builder.field("checks").startArray();

        for (Check check : checks) {
            check.toXContent(builder, params);
        }

        builder.endArray();

        if (severityMapping != null) {
            builder.field("severity");
            severityMapping.toXContent(builder, params);
        }

        builder.field("actions").startArray();

        for (AlertAction action : actions) {
            action.toXContent(builder, params);
        }
        builder.endArray();

        List<ResolveAction> resolveActions = getNonGeneratedResolveActions();

        if (resolveActions != null && resolveActions.size() > 0) {
            builder.field("resolve_actions").startArray();

            for (ResolveAction action : resolveActions) {
                action.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (params.paramAsBoolean("include_active", true)) {
            builder.field("active", active);
        }

        builder.field("log_runtime_data", logRuntimeData);

        if (params.paramAsBoolean("include_meta", true)) {
            builder.field("_meta");
            meta.toXContent(builder, params);
        }

        if (this.ui != null && this.ui.size() > 0) {
            builder.field("_ui", ui);
        }

        builder.endObject();
        return builder;
    }

    public DurationExpression getThrottlePeriod() {
        return throttlePeriod;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isLogRuntimeData() {
        return logRuntimeData;
    }

    public void setLogRuntimeData(boolean logRuntimeData) {
        this.logRuntimeData = logRuntimeData;
    }

    public Map<String, Object> getUi() {
        return ui;
    }

    public void setUi(Map<String, Object> ui) {
        this.ui = ui;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public SeverityMapping getSeverityMapping() {
        return severityMapping;
    }

    public void setSeverityMapping(SeverityMapping severityMapping) {
        this.severityMapping = severityMapping;
    }

    public List<ResolveAction> getResolveActions() {
        return resolveActions;
    }

    public void setResolveActions(List<ResolveAction> resolveActions) {
        this.resolveActions = resolveActions;
    }

    public Meta getMeta() {
        return meta;
    }

    @Override
    public String getAuthToken() {
        return meta.authToken;
    }

    public Schedule getSchedule() {
        return schedule;
    }

    public void setSchedule(Schedule schedule) {
        this.schedule = schedule;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public static Watch parse(WatchInitializationService ctx, String tenant, String id, String json, long version) throws ConfigValidationException {
        return parse(ctx, tenant, id, ValidatingJsonParser.readTree(json), version);
    }

    public static Watch parseFromElasticDocument(WatchInitializationService ctx, String tenant, String id, String json, long version)
            throws ConfigValidationException {

        JsonNode jsonNode = ValidatingJsonParser.readTree(json);

        if (jsonNode.hasNonNull("_source")) {
            return parse(ctx, tenant, id, jsonNode.get("_source"), version);
        } else {
            throw new ConfigValidationException(new MissingAttribute("_source", jsonNode));
        }
    }

    public static Watch parse(WatchInitializationService ctx, String tenant, String id, JsonNode jsonNode) throws ConfigValidationException {
        return parse(ctx, tenant, id, jsonNode, -1);
    }

    public static Watch parse(WatchInitializationService ctx, String tenant, String id, JsonNode jsonNode, long version)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);
        boolean severityHasErrors = false;

        vJsonNode.used("trigger", "_tenant");

        Watch result = new Watch();
        JobKey jobKey = createJobKey(id);

        result.jobKey = jobKey;
        result.tenant = tenant;

        if (vJsonNode.hasNonNull("description")) {
            result.description = vJsonNode.get("description").asText();
        }

        try {
            ObjectNode triggerNode = vJsonNode.getObjectNode("trigger");

            if (triggerNode != null) {
                result.schedule = DefaultScheduleFactory.INSTANCE.create(jobKey, triggerNode);
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("trigger", e);
        }

        try {
            if (vJsonNode.hasNonNull("inputs")) {
                result.checks = Check.create(ctx, (ArrayNode) vJsonNode.get("inputs"));
            } else if (vJsonNode.hasNonNull("checks")) {
                result.checks = Check.create(ctx, (ArrayNode) vJsonNode.get("checks"));
            } else {
                result.checks = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("checks", e);
        }

        try {
            if (vJsonNode.hasNonNull("severity")) {
                result.severityMapping = SeverityMapping.create(ctx, vJsonNode.get("severity"));
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("severity", e);
            severityHasErrors = true;
        }

        try {
            if (vJsonNode.hasNonNull("actions")) {
                result.actions = AlertAction.createFromArray(ctx, (ArrayNode) vJsonNode.get("actions"),
                        !severityHasErrors ? result.severityMapping : SeverityMapping.DUMMY_MAPPING);
            } else {
                result.actions = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("actions", e);
        }

        try {
            if (vJsonNode.hasNonNull("resolve_actions")) {
                result.resolveActions = ResolveAction.createFromArray(ctx, (ArrayNode) vJsonNode.get("resolve_actions"),
                        !severityHasErrors ? result.severityMapping : SeverityMapping.DUMMY_MAPPING);
            } else {
                result.resolveActions = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("resolve_actions", e);
        }

        result.throttlePeriod = vJsonNode.durationExpression("throttle_period");

        if (vJsonNode.hasNonNull("active")) {
            result.active = vJsonNode.get("active").booleanValue();
        } else {
            result.active = true;
        }

        if (vJsonNode.hasNonNull("log_runtime_data")) {
            result.logRuntimeData = vJsonNode.get("log_runtime_data").booleanValue();
        } else {
            result.logRuntimeData = false;
        }

        // XXX uargh
        result.initAutoResolveActions();

        result.version = version;

        if (vJsonNode.hasNonNull("_meta")) {
            result.meta = Meta.parseMeta(vJsonNode.get("_meta"));
        }

        if (vJsonNode.hasNonNull("_ui")) {
            JsonNode ui = vJsonNode.get("_ui");

            result.ui = JacksonTools.toMap(ui);
        }

        vJsonNode.validateUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static JobKey createJobKey(String id) {
        return new JobKey(id, "lrt");
    }

    public static Map<String, Object> getIndexMapping() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);
        result.put(new NestedValueMap.Path("properties", "checks"), Check.getIndexMapping());
        result.put(new NestedValueMap.Path("properties", "_tenant", "type"), "text");
        result.put(new NestedValueMap.Path("properties", "_tenant", "analyzer"), "keyword");
        result.put(new NestedValueMap.Path("properties", "actions", "dynamic"), true);
        result.put(new NestedValueMap.Path("properties", "actions", "properties", "checks"), Check.getIndexMapping());

        return result;
    }

    public static class Meta implements ToXContentObject {
        private String authToken;
        private String lastEditByUser;
        private Date lastEditByDate;

        static Meta parseMeta(JsonNode metaNode) {

            Meta result = new Meta();

            if (metaNode.hasNonNull("auth_token")) {
                result.authToken = metaNode.get("auth_token").asText();
            }

            if (metaNode.hasNonNull("last_edit")) {
                JsonNode lastEditNode = metaNode.get("last_edit");

                if (lastEditNode.hasNonNull("user")) {
                    result.lastEditByUser = lastEditNode.get("user").asText();
                }

                if (lastEditNode.hasNonNull("date")) {
                    // XXX not nice
                    try {
                        result.lastEditByDate = Date.from(
                                ZonedDateTime.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(lastEditNode.get("date").asText())).toInstant());
                    } catch (Exception e) {
                        log.warn("Error while parsing last edit date: " + lastEditNode + " for " + result, e);
                    }
                }

            }

            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (params.paramAsBoolean("include_auth_token", true)) {
                if (authToken != null) {
                    builder.field("auth_token", authToken);
                }
            }

            if (lastEditByUser != null) {
                builder.startObject("last_edit");
                builder.field("user", lastEditByUser);
                builder.field("date", lastEditByDate);
                builder.endObject();
            }

            builder.endObject();

            return builder;
        }

        public JsonNode toJsonNode() throws IOException {
            return JacksonXContent.toJsonNode(this);
        }

        public String getAuthToken() {
            return authToken;
        }

        public void setAuthToken(String authToken) {
            this.authToken = authToken;
        }

        public String getLastEditByUser() {
            return lastEditByUser;
        }

        public void setLastEditByUser(String lastEditByUser) {
            this.lastEditByUser = lastEditByUser;
        }

        public Date getLastEditByDate() {
            return lastEditByDate;
        }

        public void setLastEditByDate(Date lastEditByDate) {
            this.lastEditByDate = lastEditByDate;
        }

    }

    public static class JobConfigFactory extends AbstractJobConfigFactory<Watch> {
        private final WatchInitializationService initContext;
        private final String tenantIdPrefix;
        private final String tenant;

        public JobConfigFactory(String tenant, String tenantIdPrefix, WatchInitializationService initContext) {
            super(WatchRunner.class, DefaultScheduleFactory.INSTANCE);
            this.initContext = initContext;
            this.tenant = tenant;
            this.tenantIdPrefix = tenantIdPrefix;
        }

        @Override
        protected Watch createFromJsonNode(String id, JsonNode jsonNode, long version) throws ConfigValidationException {
            String tenant = jsonNode.get("_tenant").asText();

            if (this.tenant != null && !this.tenant.equals(tenant)) {
                throw new IllegalStateException("Watch " + id + " has unexpected tenant: " + tenant + "; expected: " + this.tenant);
            }

            String watchId = getWatchId(id);

            return Watch.parse(initContext, tenant, watchId, jsonNode, version);
        }

        @Override
        protected JobKey getJobKey(String id, JsonNode jsonNode) {
            return createJobKey(getWatchId(id));
        }

        private String getWatchId(String id) {
            if (this.tenantIdPrefix != null && !id.startsWith(tenantIdPrefix)) {
                throw new IllegalStateException("Watch " + id + " has unexpected tenant prefix in id: " + id + "; expected: " + this.tenantIdPrefix);
            }

            return id.substring(tenantIdPrefix.length());
        }
    }

    public static class HiddenAttributes {

        public static final List<String> LIST = Arrays.asList("_meta.auth_token");
        public static final FetchSourceContext FETCH_SOURCE_CONTEXT = new FetchSourceContext(true, Strings.EMPTY_ARRAY, asArray());

        public static String[] asArray() {
            return LIST.toArray(new String[LIST.size()]);
        }
    }
}
