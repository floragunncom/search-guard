package com.floragunn.signals.script.types;

import java.util.Map;

import com.floragunn.signals.script.SignalsScriptContextFactory;
import org.elasticsearch.script.ScriptContext;

import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.SignalsScript;

public abstract class SignalsObjectFunctionScript extends SignalsScript {

    public static final String[] PARAMETERS = {};

    public SignalsObjectFunctionScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
        super(params, watchRuntimeContext);
    }

    public abstract Object execute();

    public interface Factory {
        SignalsObjectFunctionScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
    }

    public static ScriptContext<Factory> CONTEXT = SignalsScriptContextFactory.scriptContextFor("signals_object_function", Factory.class);
}
