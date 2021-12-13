package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SearchTemplateInput extends AbstractSearchInput {

    static Check create(WatchInitializationService watchInitService, DocNode jsonObject) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type", "request");

        String name = vJsonNode.get("name").asString();
        String target = vJsonNode.get("target").asString();
        String id = vJsonNode.get("id").required().asString();

        List<String> indices = vJsonNode.get("request.indices").asListOfStrings();
        DocNode template = jsonObject.getAsNode("request.template");

        if (template == null || template.isNull()) {
            validationErrors.add(new MissingAttribute("request.template", jsonObject));
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

        SearchTemplateInput result = new SearchTemplateInput(name, target, indices, id,
                template.get("params") != null ? template.getAsNode("params").toMap() : null);

        result.timeout = timeout;
        result.searchType = searchType;
        result.indicesOptions = indicesOptions;
        result.compileScripts(watchInitService);

        return result;
    }

    private String id;
    private Map<String, Object> params;

    public SearchTemplateInput(String name, String target, List<String> indices, String id, Map<String, Object> params) {
        super(name, target, indices);
        this.id = id;
        this.params = params;
    }

    @Override
    protected Script createTemplateScript() {
        // TODO inline:  ScriptType.INLINE.getParseField()

        return new Script(ScriptType.STORED, Script.DEFAULT_TEMPLATE_LANG, this.id, this.params);
    }

    @Override
    protected Map<String, Object> getTemplateScriptParamsAsMap(WatchExecutionContext ctx) {

        Map<String, Object> result = super.getTemplateScriptParamsAsMap(ctx);
        result.putAll(params);

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
        builder.startObject("template");
        builder.field("id", this.id);
        builder.field("params");
        builder.map(this.params);
        builder.endObject();

        builder.endObject();

        if (indicesOptions != null) {
            builder.field("indices_options", indicesOptions);
        }

        return builder;
    }

}
