package com.floragunn.signals.script.types;

import java.util.Map;

import org.opensearch.script.ScriptContext;

import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.SignalsScript;

public abstract class SignalsObjectFunctionScript extends SignalsScript {

    public static final String[] PARAMETERS = {};

    public SignalsObjectFunctionScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
        super(params, watchRuntimeContext);
    }

    public abstract Object execute();

    public static interface Factory {
        SignalsObjectFunctionScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
    }

    public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("signals_object_function", Factory.class);
}
