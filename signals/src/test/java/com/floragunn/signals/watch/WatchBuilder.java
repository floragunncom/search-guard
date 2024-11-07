package com.floragunn.signals.watch;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.proxy.service.HttpProxyHostRegistry;
import com.floragunn.signals.watch.common.HttpClientConfig;
import com.floragunn.signals.watch.common.HttpProxyConfig;
import com.floragunn.signals.watch.common.HttpRequestConfig;
import com.floragunn.signals.watch.common.TlsConfig;
import com.jayway.jsonpath.JsonPath;
import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import com.google.common.base.Strings;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.mockito.Mockito;
import org.quartz.CronScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.schedule.ScheduleImpl;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory.MisfireStrategy;
import com.floragunn.searchsupport.jobs.config.schedule.elements.WeeklyTrigger;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.support.NestedValueMap.Path;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.action.handlers.IndexAction;
import com.floragunn.signals.watch.action.handlers.WebhookAction;
import com.floragunn.signals.watch.action.handlers.email.EmailAction;
import com.floragunn.signals.watch.action.handlers.slack.SlackAction;
import com.floragunn.signals.watch.action.handlers.slack.SlackActionConf;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.action.invokers.ResolveAction;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.checks.Condition;
import com.floragunn.signals.watch.checks.SearchInput;
import com.floragunn.signals.watch.checks.StaticInput;
import com.floragunn.signals.watch.checks.Transform;
import com.floragunn.signals.watch.common.auth.Auth;
import com.floragunn.signals.watch.common.auth.BasicAuth;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.floragunn.signals.watch.severity.SeverityLevel;
import com.floragunn.signals.watch.severity.SeverityMapping;
import com.floragunn.signals.watch.severity.SeverityMapping.Element;

import static com.floragunn.signals.watch.common.ValidationLevel.LENIENT;
import static com.floragunn.signals.watch.common.ValidationLevel.STRICT;

// TODO split triggers and inputs into sep builders
public class WatchBuilder {
    private String name;
    private String description;
    private List<Trigger> triggers = new ArrayList<>();
    List<Check> inputs = new ArrayList<>();
    List<AlertAction> actions = new ArrayList<>();
    List<ResolveAction> resolveActions = new ArrayList<>();
    SeverityMapping severityMapping;
    DurationExpression throttlePeriod;
    private Boolean logRuntimeData;

    final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private boolean active = true;

    public WatchBuilder(String name) {
        this.name = name;
    }

    public WatchBuilder cronTrigger(String cronExpression) {
        this.triggers.add(TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build());
        return this;
    }

    public WatchBuilder atInterval(String interval) throws ConfigValidationException {
        this.triggers.add(TriggerBuilder.newTrigger().withSchedule(
                SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(DurationFormat.INSTANCE.parse(interval).toMillis()))
                .build());
        return this;
    }

    public WatchBuilder triggerNow() {
        this.triggers.add(TriggerBuilder.newTrigger().withSchedule(
                SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0).withIntervalInMilliseconds(250))
            .build());
        return this;
    }

    public WatchBuilder atMsInterval(long msInterval) {
        this.triggers.add(TriggerBuilder.newTrigger()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(msInterval)).build());
        return this;
    }

    public WatchBuilder weekly(Object... params) {
        List<DayOfWeek> on = new ArrayList<>();
        List<TimeOfDay> at = new ArrayList<>();
        TimeZone timeZone = null;

        for (Object param : params) {
            if (param instanceof DayOfWeek) {
                on.add((DayOfWeek) param);
            } else if (param instanceof TimeOfDay) {
                at.add((TimeOfDay) param);
            } else if (param instanceof TimeZone) {
                timeZone = (TimeZone) param;
            } else {
                throw new IllegalArgumentException("Unrecognized argument " + param);
            }
        }

        this.triggers.add(new WeeklyTrigger(on, at, timeZone, MisfireStrategy.EXECUTE_NOW));

        return this;
    }

    public WatchBuilder unthrottled() {
        try {
            throttlePeriod = DurationExpression.parse("0");
            return this;
        } catch (ConfigValidationException e) {
            throw new RuntimeException(e);
        }
    }

    public WatchBuilder throttledFor(String expression) throws ConfigValidationException {
        throttlePeriod = DurationExpression.parse(expression);
        return this;
    }

    public WatchBuilder throttledFor(DurationExpression expression) throws ConfigValidationException {
        throttlePeriod = expression;
        return this;
    }

    public SearchBuilder search(String... indices) {
        return new SearchBuilder(this, indices);
    }

    public SimpleInputBuilder put(String data) {
        return new SimpleInputBuilder(this, data);
    }

    public WatchBuilder checkCondition(String condition) {
        this.inputs.add(new Condition(null, condition, "painless", null));
        return this;
    }

    public WatchBuilder logRuntimeData(boolean logRuntimeData) {
        this.logRuntimeData = logRuntimeData;
        return this;
    }

    public TransformBuilder transform(String script) {
        return new TransformBuilder(this, script);
    }

    public WatchBuilder inactive() {
        active = false;
        return this;
    }

    public SeverityMappingBuilder consider(String expression) {
        return new SeverityMappingBuilder(this, expression);
    }

    public ActionBuilder then() {
        return new ActionBuilder(this, null);
    }

    public ActionBuilder when(SeverityLevel first, SeverityLevel... rest) {
        return new ActionBuilder(this, new SeverityLevel.Set(first, rest));

    }

    public Watch build() {
        Watch result = new Watch(Watch.createJobKey(name), new ScheduleImpl(triggers), inputs, severityMapping, actions, resolveActions);

        result.setDescription(description);
        result.setActive(active);
        result.setThrottlePeriod(throttlePeriod);
        if(logRuntimeData != null) {
            result.setLogRuntimeData(logRuntimeData);
        }
        return result;
    }

    public static class SeverityMappingBuilder {
        protected final WatchBuilder parent;
        protected final String expression;
        protected final List<Element> mapping = new ArrayList<>();

        SeverityMappingBuilder(WatchBuilder parent, String expression) {
            this.parent = parent;
            this.expression = expression;
        }

        public SeverityMappingElementBuilder greaterOrEqual(double threshold) {
            return new SeverityMappingElementBuilder(this, threshold);
        }

        public ActionBuilder then() {
            parent.severityMapping = new SeverityMapping(expression, null, null, mapping);
            return new ActionBuilder(parent, null);
        }

        public ActionBuilder when(SeverityLevel first, SeverityLevel... rest) {
            parent.severityMapping = new SeverityMapping(expression, null, null, mapping);
            return new ActionBuilder(parent, new SeverityLevel.Set(first, rest));

        }
    }

    public static class SeverityMappingElementBuilder {
        protected final SeverityMappingBuilder parent;
        protected final double threshold;

        SeverityMappingElementBuilder(SeverityMappingBuilder parent, double threshold) {
            this.parent = parent;
            this.threshold = threshold;
        }

        public SeverityMappingBuilder as(SeverityLevel severityLevel) {
            parent.mapping.add(new SeverityMapping.Element(new BigDecimal(String.valueOf(threshold)), severityLevel));
            return parent;
        }

    }

    public static abstract class BaseActionBuilder {
        protected final WatchBuilder parent;

        BaseActionBuilder(WatchBuilder parent) {
            this.parent = parent;
        }

        public LogActionBuilder log(String template) {
            return new LogActionBuilder(this, template);
        }

        public IndexActionBuilder index(String indexName) {
            return new IndexActionBuilder(this, indexName);
        }

        public WebhookActionBuilder postWebhook(String uri) throws URISyntaxException {
            return new WebhookActionBuilder(this, HttpRequestConfig.Method.POST, uri);
        }

        public EmailActionBuilder email(String subject) {
            return new EmailActionBuilder(this, subject);
        }

        public SlackActionBuilder slack(SlackActionConf slackActionConf) {
            return new SlackActionBuilder(this, slackActionConf);
        }

        public ActionBuilder when(SeverityLevel first, SeverityLevel... rest) {
            return new ActionBuilder(parent, new SeverityLevel.Set(first, rest));

        }

        public ResolveActionBuilder whenResolved(SeverityLevel severityLevel1, SeverityLevel... severityLevel2) {
            return new ResolveActionBuilder(parent, severityLevel1, severityLevel2);
        }

        public GenericActionBuilder act(ActionHandler actionHandler) {
            return new GenericActionBuilder(this, actionHandler);
        }

        public GenericActionBuilder act(String actionType, Object... properties) throws ConfigValidationException {
            NestedValueMap propertyMap = new NestedValueMap();

            for (int i = 0; i < properties.length; i += 2) {
                propertyMap.put(Path.parse(String.valueOf(properties[i])), properties[i + 1]);
            }
            WatchInitializationService watchInitService = new WatchInitializationService(null, null,
                Mockito.mock(TrustManagerRegistry.class), Mockito.mock(HttpProxyHostRegistry.class), null, STRICT);
            ActionHandler actionHandler = ActionHandler.factoryRegistry.get(actionType).create(
                watchInitService, DocNode.parse(Format.JSON).from(propertyMap.toJsonString()));

            return new GenericActionBuilder(this, actionHandler);
        }

        protected abstract void addActionHandler(ActionHandler actionHandler, AbstractActionBuilder abstractActionBuilder);

    }

    public static class ActionBuilder extends BaseActionBuilder {

        private SeverityLevel.Set severityLevelSet;

        ActionBuilder(WatchBuilder parent, SeverityLevel.Set severityLevelSet) {
            super(parent);
            this.severityLevelSet = severityLevelSet;
        }

        protected void addActionHandler(ActionHandler actionHandler, AbstractActionBuilder abstractActionBuilder) {
            if (actionHandler != null) {
                parent.actions.add(new AlertAction(abstractActionBuilder.name, actionHandler, abstractActionBuilder.throttlePeriod, severityLevelSet,
                        null, null, null, abstractActionBuilder.ackEnabled));
            }

        }
    }

    public static class ResolveActionBuilder extends BaseActionBuilder {

        private SeverityLevel.Set severityLevelSet;

        ResolveActionBuilder(WatchBuilder parent, SeverityLevel severityLevel1, SeverityLevel... severityLevel2) {
            super(parent);
            this.severityLevelSet = new SeverityLevel.Set(severityLevel1, severityLevel2);
        }

        protected void addActionHandler(ActionHandler actionHandler, AbstractActionBuilder abstractActionBuilder) {
            if (actionHandler != null) {
                parent.resolveActions.add(new ResolveAction(abstractActionBuilder.name, actionHandler, severityLevelSet, Collections.emptyList()));
            }
        }
    }

    public abstract static class AbstractActionBuilder {
        protected final BaseActionBuilder parent;
        protected String name;
        protected DurationExpression throttlePeriod;
        protected boolean ackEnabled = true;

        AbstractActionBuilder(BaseActionBuilder parent) {
            this.parent = parent;
        }

        public AbstractActionBuilder name(String name) {
            this.name = name;
            return this;
        }

        public AbstractActionBuilder throttledFor(String duration) throws ConfigValidationException {
            this.throttlePeriod = DurationExpression.parse(duration);
            return this;
        }

        public AbstractActionBuilder throttledFor(DurationExpression duration) throws ParseException {
            this.throttlePeriod = duration;
            return this;
        }
        
        public AbstractActionBuilder ackEnabled(boolean ackEnabled) {
            this.ackEnabled = ackEnabled;
            return this;
        }

        public BaseActionBuilder and() {
            ActionHandler actionHandler = finish();

            if (actionHandler != null) {
                parent.addActionHandler(actionHandler, this);
            }

            return parent;
        }

        public Watch build() {
            ActionHandler actionHandler = finish();

            if (actionHandler != null) {
                parent.addActionHandler(actionHandler, this);
            }

            return parent.parent.build();
        }

        protected abstract ActionHandler finish();
    }

    public static class LogActionBuilder extends AbstractActionBuilder {

        @SuppressWarnings("unused")
        private final String template;

        LogActionBuilder(BaseActionBuilder parent, String template) {
            super(parent);
            this.template = template;
        }

        protected ActionHandler finish() {
            // TODO
            return null;
        }
    }

    public static class IndexActionBuilder extends AbstractActionBuilder {

        private final String indexName;
        private RefreshPolicy refreshPolicy = null;
        private String id;

        IndexActionBuilder(BaseActionBuilder parent, String indexName) {
            super(parent);
            this.indexName = indexName;
        }

        public IndexActionBuilder refreshPolicy(RefreshPolicy refreshPolicy) {
            this.refreshPolicy = refreshPolicy;
            return this;
        }

        public IndexActionBuilder docId(String id) {
            this.id = id;
            return this;
        }

        protected ActionHandler finish() {
            IndexAction action = new IndexAction(indexName, refreshPolicy);

            if (id != null) {
                action.setDocId(id);
            }

            return action;
        }
    }

    public static class WebhookActionBuilder extends AbstractActionBuilder {
        private final HttpRequestConfig.Method method;
        private final URI uri;
        private Auth auth;
        private String body;
        private JsonPath jsonBodyFrom;
        private Map<String, String> headers = new HashMap<>();
        private String proxy;

        private TrustManagerRegistry trustManagerRegistry = Mockito.mock(TrustManagerRegistry.class);
        private HttpProxyHostRegistry httpProxyHostRegistry = Mockito.mock(HttpProxyHostRegistry.class);

        private String truststoreId;

        WebhookActionBuilder(BaseActionBuilder parent, HttpRequestConfig.Method method, String uri) throws URISyntaxException {
            super(parent);
            this.method = method;
            this.uri = new URI(uri);
        }

        public WebhookActionBuilder basicAuth(String user, String password) {
            auth = new BasicAuth(user, password);
            return this;
        }

        public WebhookActionBuilder proxy(String proxy) {
            this.proxy = proxy;
            return this;
        }

        public WebhookActionBuilder body(String body) {
            if (this.jsonBodyFrom != null) {
                throw new IllegalStateException("body and jsonBodyFrom cannot be populated at the same time");
            }
            this.body = body;
            return this;
        }

        public WebhookActionBuilder jsonBodyFrom(String jsonBodyFrom) {
            return this.jsonBodyFrom(JsonPath.compile(jsonBodyFrom));
        }

        public WebhookActionBuilder jsonBodyFrom(JsonPath jsonBodyFrom) {
            if (this.body != null) {
                throw new IllegalStateException("body and jsonBodyFrom cannot be populated at the same time");
            }
            this.jsonBodyFrom = jsonBodyFrom;
            return this;
        }

        public WebhookActionBuilder trustManagerRegistry(TrustManagerRegistry trustManagerRegistry) {
            this.trustManagerRegistry = Objects.requireNonNull(trustManagerRegistry, "Truststore pem provider is required");
            return this;
        }

        public WebhookActionBuilder truststoreId(String truststoreId) {
            this.truststoreId = truststoreId;
            return this;
        }

        protected ActionHandler finish() {
            boolean tlsIsRequired = ! Strings.isNullOrEmpty(this.truststoreId);
            TlsConfig tlsConfig = null;
            if(tlsIsRequired) {
                tlsConfig = new TlsConfig(trustManagerRegistry, STRICT);
                tlsConfig.setTruststoreId(truststoreId);
            }
            HttpProxyConfig proxyConfig = null;
            if (! Strings.isNullOrEmpty(proxy)) {
                try {
                    proxyConfig = HttpProxyConfig.create(
                            new ValidatingDocNode(DocNode.of("proxy", proxy), new ValidationErrors()),
                            httpProxyHostRegistry, LENIENT
                    );
                } catch (ConfigValidationException e) {
                    throw new RuntimeException(e);
                }
            }
            return new WebhookAction(new HttpRequestConfig(method, uri, null, null, body, jsonBodyFrom, headers, auth, null),
                    new HttpClientConfig(null, null, tlsConfig, proxyConfig));
        }
    }

    public static class EmailActionBuilder extends AbstractActionBuilder {

        private String subject;
        private String from;
        private String body;
        private String htmlBody;
        private String account;
        private List<String> to = new ArrayList<>();
        private Map<String, EmailAction.Attachment> attachments = new LinkedHashMap<>();

        EmailActionBuilder(BaseActionBuilder parent, String subject) {
            super(parent);
            this.subject = subject;
        }

        public EmailActionBuilder to(String... to) {
            this.to.addAll(Arrays.asList(to));
            return this;
        }

        public EmailActionBuilder from(String from) {
            this.from = from;
            return this;
        }

        public EmailActionBuilder body(String body) {
            this.body = body;
            return this;
        }

        public EmailActionBuilder htmlBody(String htmlBody) {
            this.htmlBody = htmlBody;
            return this;
        }

        public EmailActionBuilder account(String account) {
            this.account = account;
            return this;
        }

        public EmailActionBuilder attach(String name, EmailAction.Attachment attachment) {
            this.attachments.put(name, attachment);
            return this;
        }

        protected ActionHandler finish() {
            EmailAction result = new EmailAction();

            result.setAccount(account);
            result.setSubject(subject);
            result.setBody(body);
            result.setHtmlBody(htmlBody);
            result.setFrom(from);
            result.setTo(to);
            result.setAttachments(attachments);

            return result;
        }
    }

    public static class GenericActionBuilder extends AbstractActionBuilder {

        private final ActionHandler actionHandler;

        GenericActionBuilder(BaseActionBuilder parent, ActionHandler actionHandler) {
            super(parent);
            this.actionHandler = actionHandler;
        }

        protected ActionHandler finish() {
            return actionHandler;
        }
    }

    public static class SlackActionBuilder extends AbstractActionBuilder {

        private final SlackActionConf slackActionConf;

        SlackActionBuilder(BaseActionBuilder parent, SlackActionConf slackActionConf) {
            super(parent);
            this.slackActionConf = slackActionConf;
        }

        protected ActionHandler finish() {
            return new SlackAction(slackActionConf);
        }
    }

    public static class SearchBuilder {
        private WatchBuilder parent;
        private String[] indices;
        private String body;
        private String query;

        private String aggregation;
        private ObjectNode bodyNode = WatchBuilder.OBJECT_MAPPER.createObjectNode();

        SearchBuilder(WatchBuilder parent, String... indices) {
            this.parent = parent;
            this.indices = indices;
        }

        public SearchBuilder attr(String key, String value) {
            bodyNode.put(key, value);
            return this;
        }

        public SearchBuilder attr(String key, int value) {
            bodyNode.put(key, value);
            return this;
        }

        /**
         * Define max number of results returned by query
         * @param size max number of results returned by query
         */
        public SearchBuilder size(int size) {
            return attr("size", size);
        }

        public SearchBuilder query(String query) {
            this.query = query;
            return this;
        }

        public SearchBuilder aggregation(DocNode aggregation) {
            this.aggregation = Objects.requireNonNull(aggregation, "Aggregation doc node is required").toJsonString();
            return this;
        }

        public WatchBuilder as(String name) throws JsonProcessingException, IOException {

            if (body == null) {
                body = buildBody();
            }

            SearchInput searchInput = new SearchInput(name, name, Arrays.asList(indices), body);

            parent.inputs.add(searchInput);

            return parent;
        }

        private String buildBody() throws JsonProcessingException, IOException {
            if(!Strings.isNullOrEmpty(this.query)) {
                JsonNode jsonNode = WatchBuilder.OBJECT_MAPPER.readTree(this.query);
                bodyNode.set("query", jsonNode);
            }
            if(!Strings.isNullOrEmpty(this.aggregation)) {
                bodyNode.set("aggs", WatchBuilder.OBJECT_MAPPER.readTree(this.aggregation));
            }
            return WatchBuilder.OBJECT_MAPPER.writeValueAsString(bodyNode);
        }
    }

    public static class SimpleInputBuilder {
        private WatchBuilder parent;
        private String data;
        private String name;

        SimpleInputBuilder(WatchBuilder parent, String data) {
            this.parent = parent;
            this.data = data;
        }

        public SimpleInputBuilder name(String name) {
            this.name = name;
            return this;
        }

        public WatchBuilder as(String name) throws JsonProcessingException, IOException {

            @SuppressWarnings("unchecked")
            Map<String, Object> map = WatchBuilder.OBJECT_MAPPER.convertValue(WatchBuilder.OBJECT_MAPPER.readTree(this.data), Map.class);

            StaticInput simpleInput = new StaticInput(this.name != null ? this.name : name, name, map);

            parent.inputs.add(simpleInput);

            return parent;
        }

    }

    public static class TransformBuilder {
        private WatchBuilder parent;
        private String script;

        TransformBuilder(WatchBuilder parent, String script) {
            this.parent = parent;
            this.script = script;
        }

        public WatchBuilder as(String name) throws JsonProcessingException, IOException {

            Transform transform = new Transform(name, name, script, "painless", Collections.emptyMap());

            parent.inputs.add(transform);

            return parent;
        }

    }
}