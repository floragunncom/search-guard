package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.SignalsScript;
import com.floragunn.signals.watch.init.WatchInitializationService;
import com.google.common.base.Strings;

public class Condition extends Check {

    private String source;
    private String lang;
    private Map<String, Object> params;
    private Script script;
    private ConditionScript.Factory scriptFactory;

    public Condition(String name, String source, String lang, Map<String, Object> params) {
        super(name);
        this.source = source;
        this.lang = lang;
        this.params = params != null ? params : Collections.emptyMap();

        script = new Script(ScriptType.INLINE, lang != null ? lang : "painless", source, this.params);
    }

    static Condition create(WatchInitializationService watchInitService, DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.get("name").asString();
        String source = vJsonNode.get("source").asString();
        String lang = vJsonNode.get("lang").asString();

        Map<String, Object> params = jsonObject.getAsNode("params") != null ? jsonObject.getAsNode("params").toMap() : null;

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        Condition result = new Condition(name, source, lang, params);

        result.compileScripts(watchInitService);

        return result;
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.scriptFactory = watchInitService.compile("source", script, ConditionScript.CONTEXT, validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    public String getSource() {
        return source;
    }

    public String getLang() {
        return lang;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", "condition");

        if (!Strings.isNullOrEmpty(name)) {
            builder.field("name", name);
        }

        if (!Strings.isNullOrEmpty(lang)) {
            builder.field("lang", lang);
        }

        if (!Strings.isNullOrEmpty(source)) {
            builder.field("source", source);
        }

        if (this.params != null && this.params.size() > 0) {
            builder.field("params", this.params);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) throws CheckExecutionException {
        try {
            ConditionScript conditionScript = scriptFactory.newInstance(script.getParams(), ctx);
            return conditionScript.execute();
        } catch (ScriptException e) {
            throw new CheckExecutionException(this, "Script Execution Error", e);
        }
    }

    public static abstract class ConditionScript extends SignalsScript {

        public static final String[] PARAMETERS = {};

        public ConditionScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
            super(params, watchRuntimeContext);
        }

        public abstract boolean execute();

        public static interface Factory {
            ConditionScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
        }

        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("signals_condition", Factory.class);

    }

}
