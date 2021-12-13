package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.json.JacksonTools;
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

    static Transform create(WatchInitializationService watchInitService, ObjectNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String name = vJsonNode.string("name");
        String target = vJsonNode.string("target");
        String lang = vJsonNode.string("lang");
        String source = vJsonNode.string("source");

        Map<String, Object> params = JacksonTools.toMap(vJsonNode.get("params"));

        vJsonNode.validateUnusedAttributes();

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

        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("signals_transform", Factory.class);

    }
}
