package com.floragunn.signals.script;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.TemplateScript;

public class SignalsScriptContextFactory {

    public static final ScriptContext<TemplateScript.Factory> TEMPLATE_CONTEXT = scriptContextFor("signals_template", TemplateScript.Factory.class);

    public static <T> ScriptContext<T> scriptContextFor(String name, Class<T> factory) {
        return new ScriptContext<>(
                name,
                factory,
                200,
                TimeValue.timeValueMillis(0),
                false,
                true
        );
    }

}
