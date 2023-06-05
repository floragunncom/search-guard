/*
 * Copyright 2019-2022 floragunn GmbH
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchsupport.jobs.config.schedule.ScheduleImpl;
import com.floragunn.signals.actions.watch.generic.service.persistence.WatchInstanceData;
import com.floragunn.signals.watch.common.throttle.ThrottlePeriodParser;
import com.floragunn.signals.watch.common.InstanceParser;
import com.floragunn.signals.watch.common.Instances;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.Trigger;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchsupport.jobs.config.AbstractJobConfigFactory;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory;
import com.floragunn.searchsupport.jobs.config.schedule.Schedule;
import com.floragunn.signals.NoSuchActionException;
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

import static com.floragunn.signals.watch.WatchInstanceIdService.INSTANCE_ID_SEPARATOR;

public class Watch extends WatchElement implements JobConfig, ToXContentObject, Cloneable {

    public static final String GENERIC_WATCH_INSTANCE_ID_PARAMETER = "id";

    public enum WatchType {
        SINGLE_INSTANCE(true),
        GENERIC(false),
        GENERIC_INSTANCE(true);

        private final boolean executable;

        WatchType(boolean executable) {
            this.executable = executable;
        }

        public boolean isExecutable() {
            return executable;
        }
    }
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
    private Instances instances;
    private String instanceId;

    private com.floragunn.fluent.collections.ImmutableMap<String, Object> instanceParameters;
    private long version;

    public Watch() {
    }

    public Watch(JobKey jobKey, Schedule schedule, List<Check> checks, SeverityMapping severityMapping, List<AlertAction> actions,
            List<ResolveAction> resolveActions, Instances instances) {
        this.jobKey = jobKey;
        this.schedule = schedule;
        this.checks = checks;
        this.severityMapping = severityMapping;
        this.actions = actions;
        this.resolveActions = resolveActions;
        this.instances = Objects.requireNonNull(instances, "Watch instances is required");
    }

    public String getId() {
        return jobKey.getName();
    }

    public Optional<String> getParentGenericWatchId() {
        return Optional.of(this) //
            .filter(watch -> WatchType.GENERIC_INSTANCE.equals(watch.getWatchType())) //
            .map(Watch::getId) //
            .map(WatchInstanceIdService::extractParentGenericWatchId);
    }

    public Instances getInstanceDefinition() {
        return instances;
    }

    public Optional<String> getInstanceId() {
        return Optional.ofNullable(instanceId);
    }

    public com.floragunn.fluent.collections.ImmutableMap<String, Object> getInstanceParameters() {
        return instanceParameters == null ? com.floragunn.fluent.collections.ImmutableMap.empty() : instanceParameters;
    }

    private void setInstancesParameters(WatchInstanceData instancesParameters) {
        if(instancesParameters != null) {
            this.instanceId = Objects.requireNonNull(instancesParameters.getInstanceId(), "Generic watch instance id is required");
            this.instanceParameters = Objects.requireNonNull(instancesParameters.getParameters(), "Watch parameters are required")//
                .with(GENERIC_WATCH_INSTANCE_ID_PARAMETER, instanceId);
        }
    }

    private long computeVersion(long watchVersion, long parametersVersion) {
        log.debug("Watch version '{}', parameters version '{}'.", watchVersion, parametersVersion);
        if(watchVersion < 0) {
            throw new IllegalArgumentException("Watch version " + watchVersion + " is below 0");
        }
        if(parametersVersion < 0) {
            throw new IllegalArgumentException("Parameters version " + parametersVersion + " is below 0");
        }
        watchVersion = watchVersion << 32;
        return watchVersion | parametersVersion;
    }

    Watch createInstance(WatchInstanceData watchInstanceData) throws CloneNotSupportedException, IOException, ConfigValidationException {
        if(!(instances.isEnabled() || (getWatchType().equals(WatchType.GENERIC)))) {
            throw new IllegalArgumentException("It is possible to create instance only of generic watch");
        }
        Watch clone = this.clone();
        Objects.requireNonNull(watchInstanceData, "Watch instance data is required");
        clone.instanceId = Objects.requireNonNull(watchInstanceData.getInstanceId(), "Generic watch instance id is required");
        clone.instanceParameters = Objects.requireNonNull(watchInstanceData.getParameters(), "Watch parameters are required")//
            .with(GENERIC_WATCH_INSTANCE_ID_PARAMETER, watchInstanceData.getInstanceId());
        clone.version = computeVersion(clone.version, watchInstanceData.getVersion());
        String newId = Watch.createInstanceId(clone.getId(), watchInstanceData.getInstanceId());
        clone.jobKey = createJobKey(newId);
        ValidationErrors validationErrors = new ValidationErrors();
        // it is necessary to clone schedulers with a new job key because otherwise watch will be not executed
        clone.schedule = clone.cloneSchedule(validationErrors);
        validationErrors.throwExceptionForPresentErrors();
        return clone;
    }

    private Schedule cloneSchedule(ValidationErrors validationErrors) throws IOException, DocumentParseException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        this.scheduleToXContent(builder);
        builder.endObject();
        String triggerJson = BytesReference.bytes(builder).utf8ToString();
        DocNode docNode = DocNode.parse(Format.JSON).from(triggerJson);
        ValidatingDocNode vJsonNode = new ValidatingDocNode(docNode, validationErrors);
        return vJsonNode.get("trigger").by((triggerNode) -> DefaultScheduleFactory.INSTANCE.create(jobKey, triggerNode));
    }

    public static String createInstanceId(String watchId, String instanceId) {
        return WatchInstanceIdService.createInstanceId(watchId, instanceId);
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
        if (schedule != null) {
            return schedule.getTriggers();
        } else {
            return Collections.emptyList();
        }
    }

    public List<Check> getChecks() {
        return checks;
    }

    public List<AlertAction> getActions() {
        return actions;
    }
    
    public AlertAction getActionByName(String name) throws NoSuchActionException {
        for (AlertAction action : this.actions) {
            if (name.equals(action.getName())) {
                return action;
            }
        }
        
        throw new NoSuchActionException(getId(), name);
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
        if((WatchType.SINGLE_INSTANCE == getWatchType()) || (WatchType.GENERIC == getWatchType())) {
            return getIdAndHash();
        }
        // so this code is executed only for watch of type GENERIC_INSTANCE>
        return WatchInstanceIdService.extractParentGenericWatchId(getId()) + "." + secureHash();
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

        if((instances != null) && (instances.isEnabled())) {
            builder.field(InstanceParser.FIELD_INSTANCES, instances);
        }

        scheduleToXContent(builder);

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

    private void scheduleToXContent(XContentBuilder builder) throws IOException {
        if (schedule != null) {
            builder.field("trigger");
            builder.startObject();

            builder.field("schedule", schedule);

            builder.endObject();
        }
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

    private WatchType getWatchType() {
        if(instances.isEnabled() && Objects.nonNull(instanceId)) {
            return WatchType.GENERIC_INSTANCE;
        } else if(instances.isEnabled()) {
            return WatchType.GENERIC;
        } else {
            return WatchType.SINGLE_INSTANCE;
        }
    }

    @Override
    public boolean isExecutable() {
        return getWatchType().isExecutable();
    }

    @Override
    public boolean isGenericJobConfig() {
        return getWatchType() == WatchType.GENERIC;
    }

    public static Watch parse(WatchInitializationService ctx, String tenant, String id, String json, long version) throws ConfigValidationException {
        return parse(ctx, tenant, id, DocNode.parse(Format.JSON).from(json), version);
    }

    public static Watch parseFromElasticDocument(WatchInitializationService ctx, String tenant, String id, String json, long version)
            throws ConfigValidationException {

        DocNode jsonNode = DocNode.parse(Format.JSON).from(json);

        if (jsonNode.hasNonNull("_source")) {
            return parse(ctx, tenant, id, jsonNode.getAsNode("_source"), version);
        } else {
            throw new ConfigValidationException(new MissingAttribute("_source", jsonNode));
        }
    }

    public static Watch parse(WatchInitializationService ctx, String tenant, String id, DocNode jsonNode) throws ConfigValidationException {
        return parse(ctx, tenant, id, jsonNode, -1);
    }

    public static Watch parse(WatchInitializationService ctx, String tenant, String id, DocNode jsonNode, long version)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);
        ThrottlePeriodParser throttlePeriodParser = ctx.getThrottlePeriodParser();
        boolean severityHasErrors = false;

        vJsonNode.used("trigger", "_tenant", "_name");

        Watch result = new Watch();
        JobKey jobKey = createJobKey(id);

        result.jobKey = jobKey;
        result.tenant = tenant;

        if (vJsonNode.hasNonNull("description")) {
            result.description = vJsonNode.get("description").asString();
        }

        result.schedule = vJsonNode.get("trigger").by((triggerNode) -> DefaultScheduleFactory.INSTANCE.create(jobKey, triggerNode));

        InstanceParser instanceParser = new InstanceParser(validationErrors);
        result.instances = instanceParser.parse(vJsonNode);
        if(result.instances.isEnabled()) {
            if(id.contains(INSTANCE_ID_SEPARATOR)) {
                String message = "Generic watch id cannot contain '" + INSTANCE_ID_SEPARATOR + "' string.";
                validationErrors.add(new ValidationError(GENERIC_WATCH_INSTANCE_ID_PARAMETER, message));
            }
        }
        
        try {
            if (vJsonNode.get("inputs").asAnything() instanceof List) {
                result.checks = Check.create(ctx, (List<?>) jsonNode.get("inputs"));
            } else if (vJsonNode.get("checks").asAnything() instanceof List) {
                result.checks = Check.create(ctx, (List<?>) jsonNode.get("checks"));
            } else {
                result.checks = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("checks", e);
        }

        try {
            if (vJsonNode.hasNonNull("severity")) {
                result.severityMapping = SeverityMapping.create(ctx, jsonNode.getAsNode("severity"));
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("severity", e);
            severityHasErrors = true;
        }

        try {
            if (vJsonNode.hasNonNull("actions")) {
                result.actions = AlertAction.createFromArray(ctx, jsonNode.getAsListOfNodes("actions"),
                        !severityHasErrors ? result.severityMapping : SeverityMapping.DUMMY_MAPPING);
            } else {
                result.actions = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("actions", e);
        }

        try {
            if (vJsonNode.hasNonNull("resolve_actions")) {
                result.resolveActions = ResolveAction.createFromArray(ctx, jsonNode.getAsListOfNodes("resolve_actions"),
                        !severityHasErrors ? result.severityMapping : SeverityMapping.DUMMY_MAPPING);
            } else {
                result.resolveActions = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("resolve_actions", e);
        }

        result.throttlePeriod = vJsonNode.get("throttle_period")
                .withDefault(throttlePeriodParser.getDefaultThrottle())
                .byString(throttlePeriodParser::parseThrottle);

        if (vJsonNode.hasNonNull("active")) {
            result.active = vJsonNode.get("active").asBoolean();
        } else {
            result.active = true;
        }

        if (vJsonNode.hasNonNull("log_runtime_data")) {
            result.logRuntimeData = vJsonNode.get("log_runtime_data").asBoolean();
        } else {
            result.logRuntimeData = false;
        }

        // XXX uargh
        result.initAutoResolveActions();

        result.version = version;

        if (vJsonNode.hasNonNull("_meta")) {
            result.meta = Meta.parseMeta(vJsonNode.get("_meta").asDocNode());
        }

        if (vJsonNode.hasNonNull("_ui")) {
            result.ui = vJsonNode.get("_ui").asMap();
        }

        vJsonNode.checkForUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    @Override
    protected Watch clone() throws CloneNotSupportedException {
        Watch copy = (Watch) super.clone();
        copy.jobKey = createJobKey(copy.getId());
        copy.jobDataMap = copy.jobDataMap == null ? null : new HashMap<>(copy.jobDataMap);
        copy.schedule = new ScheduleImpl(new ArrayList<>(this.schedule.getTriggers()));
        copy.checks = copy.checks == null ? null : new ArrayList<>(copy.checks);
        copy.actions = copy.actions == null ? null : new ArrayList<>(copy.actions);
        copy.resolveActions = copy.resolveActions == null ? null : new ArrayList<>(copy.resolveActions);
        copy.ui = copy.ui == null ? null : new HashMap<>(copy.ui);
        copy.instances = copy.instances == null ? null : new Instances(copy.instances.isEnabled(), copy.instances.getParams());
        return copy;
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
    
    public static Map<String, Object> getIndexMappingUpdate() {
        NestedValueMap result = new NestedValueMap();

        result.put("dynamic", true);
        result.put(new NestedValueMap.Path("properties", "checks"), Check.getIndexMappingUpdate());
        result.put(new NestedValueMap.Path("properties", "actions", "properties", "checks"), Check.getIndexMappingUpdate());

        return result;
    }

    public static class Meta implements ToXContentObject {
        private String authToken;
        private String lastEditByUser;
        private Date lastEditByDate;

        static Meta parseMeta(DocNode metaNode) {

            Meta result = new Meta();

            if (metaNode.hasNonNull("auth_token")) {
                result.authToken = metaNode.getAsString("auth_token");
            }

            if (metaNode.hasNonNull("last_edit")) {
                DocNode lastEditNode = metaNode.getAsNode("last_edit");

                if (lastEditNode.hasNonNull("user")) {
                    result.lastEditByUser = lastEditNode.getAsString("user");
                }

                if (lastEditNode.hasNonNull("date")) {
                    // XXX not nice
                    try {
                        result.lastEditByDate = Date.from(
                                ZonedDateTime.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(lastEditNode.getAsString("date"))).toInstant());
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

        public Map<String, Object> toMap() throws IOException {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();

            if (authToken != null) {
                result.put("auth_token", authToken);
            }

            if (lastEditByUser != null) {
                result.put("last_edit", ImmutableMap.of("user", lastEditByUser, "date", lastEditByDate));
            }

            return result;
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
        protected Watch createFromJsonNode(String id, DocNode jsonNode, long version) throws ConfigValidationException {
            String tenant = jsonNode.getAsString("_tenant");

            if (this.tenant != null && !this.tenant.equals(tenant)) {
                throw new IllegalStateException("Watch " + id + " has unexpected tenant: " + tenant + "; expected: " + this.tenant);
            }

            String watchId = getWatchId(id);
            log.debug("Parse watch with id '{}' and watch initialization service '{}'", id, initContext);
            return Watch.parse(initContext, tenant, watchId, jsonNode, version);
        }

        @Override
        protected JobKey getJobKey(String id, DocNode jsonNode) {
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

        public static final List<String> LIST = Arrays.asList("_meta.auth_token", "_name");
        public static final FetchSourceContext FETCH_SOURCE_CONTEXT = new FetchSourceContext(true, Strings.EMPTY_ARRAY, asArray());

        public static String[] asArray() {
            return LIST.toArray(new String[LIST.size()]);
        }
    }

    public void setThrottlePeriod(DurationExpression throttlePeriod) {
        this.throttlePeriod = throttlePeriod;
    }
}
