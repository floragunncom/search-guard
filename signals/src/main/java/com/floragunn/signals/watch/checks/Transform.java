package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.floragunn.signals.script.SignalsScriptContextFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.SignalsScript;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class Transform extends AbstractInput {

    private String source;
    private String lang;
    private Map<String, Object> params;
    private Script script;
    private TransformScript.Factory scriptFactory;

    public Transform(String name, String target, String source, String lang, Map<String, Object> params) {
        super(name, target);
        this.source = source;
        this.lang = lang;
        this.params = params != null ? params : Collections.emptyMap();

        script = new Script(ScriptType.INLINE, lang != null ? lang : "painless", source, this.params);

    }

    static Transform create(WatchInitializationService watchInitService, DocNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.get("name").asString();
        String target = vJsonNode.get("target").asString();
        String lang = vJsonNode.get("lang").asString();
        String source = vJsonNode.get("source").asString();

        Map<String, Object> params = jsonObject.getAsNode("params") != null ? jsonObject.getAsNode("params").toMap() : null;

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        Transform result = new Transform(name, target, source, lang, params);

        result.compileScripts(watchInitService);

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (name != null) {
            builder.field("name", name);
        }
        
        if (target != null) {
            builder.field("target", target);
        }

        builder.field("type", "transform");

        if (source != null) {
            builder.field("source", source);
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

    private void compileScripts(WatchInitializationService watchInitializationService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.scriptFactory = watchInitializationService.compile("source", script, TransformScript.CONTEXT, validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) throws CheckExecutionException {
        try {
            TransformScript transformScript = scriptFactory.newInstance(script.getParams(), ctx.clone());
            Object result = transformScript.execute();

            setResult(ctx, result);

            return true;
        } catch (ScriptException e) {
            throw new CheckExecutionException(this, "Script Execution Error", e);
        }
    }

    public static abstract class TransformScript extends SignalsScript {

        public static final String[] PARAMETERS = {};

        public TransformScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
            super(params, watchRuntimeContext);
        }

        public abstract Object execute();

        public static interface Factory {
            TransformScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
        }

        public static ScriptContext<Factory> CONTEXT = SignalsScriptContextFactory.scriptContextFor("signals_transform", Factory.class);

    }
}
