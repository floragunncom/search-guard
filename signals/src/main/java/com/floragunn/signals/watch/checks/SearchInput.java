package com.floragunn.signals.watch.checks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.MissingAttribute;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SearchInput extends AbstractSearchInput {

    private final String body;

    private SearchType searchType = SearchType.DEFAULT;

    public SearchInput(String name, String target, String index, String body) {
        this(name, target, Collections.singletonList(index), body);
    }

    public SearchInput(String name, String target, List<String> indices, String body) {
        this(name, target, indices, body, null, null);
        ;
    }

    public SearchInput(String name, String target, List<String> indices, String body, SearchType searchType, IndicesOptions indicesOptions) {
        super(name, target, indices);
        this.body = body;
        this.searchType = searchType;
        this.indicesOptions = indicesOptions;
    }

    @Override
    protected Script createTemplateScript() {
        return new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, this.body, Collections.emptyMap());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", "search");
        builder.field("name", name);
        builder.field("target", target);

        if (timeout != null) {
            builder.field("timeout", timeout.getStringRep());
        }

        if (searchType != null) {
            builder.field("search_type", searchType.name().toLowerCase());
        }

        builder.startObject("request");
        builder.field("indices", indices);
        builder.field("body");
        builder.rawValue(new ByteArrayInputStream(body.getBytes("utf-8")), XContentType.JSON);
        builder.endObject();

        if (indicesOptions != null) {
            builder.field("indices_options", indicesOptions);
        }

        builder.endObject();
        return builder;
    }

    static Check create(WatchInitializationService watchInitService, ObjectNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonObject, validationErrors);

        vJsonNode.used("type", "request");

        String name = null;
        String target = null;

        name = vJsonNode.string("name");
        target = vJsonNode.string("target");

        List<String> indices = JacksonTools.toStringArray(jsonObject.at("/request/indices"));
        JsonNode body = jsonObject.at("/request/body");

        if (body == null || body.isMissingNode()) {
            validationErrors.add(new MissingAttribute("request.body", jsonObject));
        }

        TimeValue timeout = vJsonNode.timeValue("timeout");
        SearchType searchType = vJsonNode.caseInsensitiveEnum("search_type", SearchType.class, null);
        IndicesOptions indicesOptions = null;

        if (vJsonNode.hasNonNull("indices_options")) {
            try {
                indicesOptions = parseIndicesOptions(vJsonNode.get("indices_options"));
            } catch (ConfigValidationException e) {
                validationErrors.add("indices_options", e);
            }
        }

        vJsonNode.validateUnusedAttributes();

        validationErrors.throwExceptionForPresentErrors();

        SearchInput result;

        try {
            result = new SearchInput(name, target, indices, DefaultObjectMapper.objectMapper.writeValueAsString(body));
        } catch (JsonProcessingException e) {
            // This should not happen
            throw new RuntimeException(e);
        }

        result.timeout = timeout;
        result.searchType = searchType;
        result.indicesOptions = indicesOptions;

        result.compileScripts(watchInitService);

        return result;

    }

    static void addIndexMappingProperties(NestedValueMap mapping) {
        mapping.put(new NestedValueMap.Path("request", "type"), "object");
        mapping.put(new NestedValueMap.Path("request", "dynamic"), true);
        mapping.put(new NestedValueMap.Path("request", "properties", "body", "type"), "object");
        mapping.put(new NestedValueMap.Path("request", "properties", "body", "dynamic"), true);
        mapping.put(new NestedValueMap.Path("request", "properties", "body", "enabled"), false);

    }

    public SearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(SearchType searchType) {
        this.searchType = searchType;
    }

    public String getBody() {
        return body;
    }

}
