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

public class Calc extends Check {

    private String source;
    private String lang;
    private Map<String, Object> params;
    private Script script;
    private CalcScript.Factory scriptFactory;

    public Calc(String name, String source, String lang, Map<String, Object> params) {
        super(name);
        this.source = source;
        this.lang = lang;
        this.params = params != null ? params : Collections.emptyMap();

        script = new Script(ScriptType.INLINE, lang != null ? lang : "painless", source, this.params);
    }

    static Calc create(WatchInitializationService watchInitService, DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.get("name").asString();
        String lang = vJsonNode.get("lang").asString();
        String source = vJsonNode.get("source").asString();

        Map<String, Object> params = jsonObject.getAsNode("params") != null ? jsonObject.getAsNode("params").toMap() : null;

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        Calc result = new Calc(name, source, lang, params);

        result.compileScripts(watchInitService);

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (name != null) {
            builder.field("name", name);
        }

        builder.field("type", "calc");

        if (source != null) {
            builder.field("source", source);
        }

        if (lang != null) {
            builder.field("lang", lang);
        }

        // TODO params
        // builder.endObject();

        builder.endObject();
        return builder;
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
    public boolean execute(WatchExecutionContext ctx) throws CheckExecutionException {
        try {
            CalcScript transformScript = scriptFactory.newInstance(script.getParams(), ctx);
            transformScript.execute();

            return true;
        } catch (ScriptException e) {
            throw new CheckExecutionException(this, "Script Execution Error", e);
        }
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.scriptFactory = watchInitService.compile("source", script, CalcScript.CONTEXT, validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    public static abstract class CalcScript extends SignalsScript {

        public static final String[] PARAMETERS = {};

        public CalcScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
            super(params, watchRuntimeContext);
        }

        public abstract Object execute();

        public static interface Factory {
            CalcScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
        }

        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("signals_calc", Factory.class);

    }
}