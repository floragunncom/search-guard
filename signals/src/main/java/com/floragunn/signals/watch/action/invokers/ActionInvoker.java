/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.watch.action.invokers;

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
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;

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
