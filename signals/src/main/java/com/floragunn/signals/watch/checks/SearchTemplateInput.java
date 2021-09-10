package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.json.JacksonTools;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class SearchTemplateInput extends AbstractSearchInput {

    static Check create(WatchInitializationService watchInitService, ObjectNode jsonObject) throws ConfigValidationException {

        ValidationErrors validationErrors = new ValidationErrors();

        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonObject, validationErrors);

        vJsonNode.used("type", "request");

        String name = vJsonNode.string("name");
        String target = vJsonNode.string("target");
        String id = vJsonNode.requiredString("id");

        List<String> indices = JacksonTools.toStringArray(jsonObject.at("/request/indices"));
        JsonNode template = jsonObject.at("/request/template");

        if (template == null) {
            validationErrors.add(new MissingAttribute("request.template", jsonObject));
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

        SearchTemplateInput result = new SearchTemplateInput(name, target, indices, id, JacksonTools.toMap(template.get("params")));

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
