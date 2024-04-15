package com.floragunn.signals.watch.checks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SearchInput extends AbstractSearchInput {

    private final String body;

    private SearchType searchType = SearchType.DEFAULT;

    public SearchInput(String name, String target, String index, String body) {
        this(name, target, Collections.singletonList(index), body, feature -> true);
    }

    public SearchInput(String name, String target, List<String> indices, String body, Predicate<NodeFeature> isFeatureSupported) {
        this(name, target, indices, body, null, null, isFeatureSupported);;
    }

    public SearchInput(String name, String target, List<String> indices, String body, SearchType searchType, IndicesOptions indicesOptions, Predicate<NodeFeature> isFeatureSupported) {
        super(name, target, indices, isFeatureSupported);
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

    static Check create(WatchInitializationService watchInitService, DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type", "request");

        String name = null;
        String target = null;

        name = vJsonNode.get("name").asString();
        target = vJsonNode.get("target").asString();

        List<String> indices = vJsonNode.get("request.indices").asListOfStrings();
        DocNode body = jsonObject.getAsNode("request").getAsNode("body");

        if (body == null || body.isNull()) {
            validationErrors.add(new MissingAttribute("request.body", jsonObject));
        }

        TimeValue timeout = vJsonNode.get("timeout").byString((v) -> TimeValue.parseTimeValue(v, "timeout"));
        SearchType searchType = vJsonNode.get("search_type").asEnum(SearchType.class);
        IndicesOptions indicesOptions = null;

        if (vJsonNode.hasNonNull("indices_options")) {
            try {
                indicesOptions = parseIndicesOptions(jsonObject.getAsNode("indices_options"));
            } catch (ConfigValidationException e) {
                validationErrors.add("indices_options", e);
            }
        }

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        Predicate<NodeFeature> isFeatureSupported = (feature) -> watchInitService.getFeatureService().clusterHasFeature(watchInitService.getClusterService().state(), feature);
        SearchInput result = new SearchInput(name, target, indices, body.toJsonString(), isFeatureSupported);

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
