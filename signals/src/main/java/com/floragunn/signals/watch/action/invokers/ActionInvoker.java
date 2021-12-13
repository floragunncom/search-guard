package com.floragunn.signals.watch.action.invokers;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.support.InlinePainlessScript;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.handlers.ActionHandler;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.init.WatchInitializationService;

public abstract class ActionInvoker implements ToXContent {
    protected final String name;
    protected final ActionHandler handler;
    protected final List<Check> checks;
    protected final InlinePainlessScript<SignalsObjectFunctionScript.Factory> foreach;
    protected final int foreachLimit;

    protected ActionInvoker(String name, ActionHandler handler, List<Check> checks, InlinePainlessScript<SignalsObjectFunctionScript.Factory> foreach,
            Integer foreachLimit) {
        this.name = name;
        this.handler = handler;
        this.checks = checks != null ? Collections.unmodifiableList(checks) : null;
        this.foreach = foreach;
        this.foreachLimit = foreachLimit != null ? foreachLimit.intValue() : 100;
    }

    public String getName() {
        return name;
    }

    public List<Check> getChecks() {
        return checks;
    }

    @Override
    public String toString() {
        return handler != null ? handler.getClass().getSimpleName() + " " + name : name;
    }

    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {
        return handler.execute(ctx);
    }

    public String toJson() {
        return Strings.toString(this);
    }

    protected static List<Check> createNestedChecks(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode,
            ValidationErrors validationErrors) {
        if (vJsonNode.hasNonNull("checks")) {
            if (vJsonNode.getDocumentNode().get("checks") instanceof List) {

                try {
                    return Check.create(watchInitService, (List<?>) vJsonNode.getDocumentNode().get("checks"));
                } catch (ConfigValidationException e) {
                    validationErrors.add("checks", e);
                }
            } else {
                validationErrors.add(new InvalidAttributeValue("checks", vJsonNode.get("checks"), "Array", vJsonNode));
            }
        }

        return Collections.emptyList();
    }

    public ActionHandler getHandler() {
        return handler;
    }

    public InlinePainlessScript<SignalsObjectFunctionScript.Factory> getForeach() {
        return foreach;
    }

    public int getForeachLimit() {
        return foreachLimit;
    }
}
