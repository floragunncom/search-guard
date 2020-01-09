package com.floragunn.signals.script;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.script.ScriptContext;

import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.watch.checks.Calc;
import com.floragunn.signals.watch.checks.Condition;
import com.floragunn.signals.watch.checks.Transform;
import com.floragunn.signals.watch.severity.SeverityMapping;

public class SignalsScriptContexts {
    public static final List<ScriptContext<?>> CONTEXTS = Arrays.asList(Condition.ConditionScript.CONTEXT, Transform.TransformScript.CONTEXT,
            Calc.CalcScript.CONTEXT, SeverityMapping.SeverityValueScript.CONTEXT, SignalsObjectFunctionScript.CONTEXT);
}
