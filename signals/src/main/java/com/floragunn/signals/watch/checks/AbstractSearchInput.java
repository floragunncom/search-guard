package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.xcontent.ObjectTreeXContent;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.init.WatchInitializationService;

public abstract class AbstractSearchInput extends AbstractInput {
    private static final Logger log = LogManager.getLogger(AbstractSearchInput.class);

    protected final List<String> indices;

    protected TimeValue timeout;
    protected TemplateScript.Factory templateScriptFactory;
    protected SearchType searchType;
    protected IndicesOptions indicesOptions;

    public AbstractSearchInput(String name, String target, List<String> indices) {
        super(name, target);
        this.indices = Collections.unmodifiableList(indices);
    }

    public List<String> getIndices() {
        return indices;
    }

    protected String[] getIndicesAsArray() {
        return this.indices.toArray(new String[this.indices.size()]);
    }

    protected abstract Script createTemplateScript();

    protected String executeTemplateScript(WatchExecutionContext ctx) {

        Map<String, Object> actualTemplateScriptParams = getTemplateScriptParamsAsMap(ctx);

        return this.templateScriptFactory.newInstance(actualTemplateScriptParams).execute();
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.templateScriptFactory = watchInitService.compile("request.body", createTemplateScript(), TemplateScript.CONTEXT, validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) throws CheckExecutionException {
        String searchBody = executeTemplateScript(ctx);

        if (log.isDebugEnabled()) {
            log.debug("Executed template script:\n" + searchBody);
        }

        return executeSearchRequest(ctx, searchBody);
    }

    protected boolean executeSearchRequest(WatchExecutionContext ctx, String searchBody) {
        SearchRequest searchRequest = createSearchRequest(ctx.getxContentRegistry(), searchBody);

        if (log.isDebugEnabled()) {
            log.debug("Executing: " + searchRequest);
        }

        SearchResponse searchResponse = ctx.getClient().search(searchRequest)
                .actionGet(timeout != null ? timeout : new TimeValue(30, TimeUnit.SECONDS));

        if (log.isDebugEnabled()) {
            log.debug("Response: " + searchResponse);
        }

        Object result = ObjectTreeXContent.toObjectTree(searchResponse, new MapParams(Collections.emptyMap()),
                () -> NestedValueMap.createNonCloningMap());
        setResult(ctx, result);

        return true;
    }

    protected SearchRequest createSearchRequest(NamedXContentRegistry xContentRegistry, String searchBody) {

        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, searchBody)) {

            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            SearchRequest result = new SearchRequest(this.getIndicesAsArray(), searchSourceBuilder);

            if (this.searchType != null) {
                result.searchType(searchType);
            }

            if (this.indicesOptions != null) {
                result.indicesOptions(indicesOptions);
            }

            return result;
        } catch (IOException e) {
            throw new RuntimeException("Error while creating search request for " + this);
        }
    }

    public static IndicesOptions parseIndicesOptions(JsonNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        EnumSet<WildcardStates> wildcards = vJsonNode.value("expand_wildcards", (s) -> WildcardStates.parseParameter(s, null), "all|open|none|closed",
                EnumSet.of(WildcardStates.OPEN));

        IndicesOptions result = IndicesOptions.fromOptions(vJsonNode.booleanAttribute("ignore_unavailable", false),
                vJsonNode.booleanAttribute("allow_no_indices", false), wildcards.contains(WildcardStates.OPEN),
                wildcards.contains(WildcardStates.CLOSED), false, false, false, false);

        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

}
