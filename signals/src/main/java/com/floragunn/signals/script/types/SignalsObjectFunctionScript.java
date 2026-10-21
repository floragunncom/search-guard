/*
 * Copyright 2020-2023 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
