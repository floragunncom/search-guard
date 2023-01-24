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
package com.floragunn.signals.support;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.watch.init.WatchInitializationService;
import java.io.IOException;
import java.util.Collections;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

public class InlinePainlessScript<Factory> implements ToXContentFragment {
    private final ScriptContext<Factory> scriptContext;
    private final String source;
    private Factory scriptFactory;

    public InlinePainlessScript(ScriptContext<Factory> scriptContext, String source) {
        this.source = source;
        this.scriptContext = scriptContext;
    }

    public void compile(WatchInitializationService watchInitializationService, ValidationErrors validationErrors) {
        this.scriptFactory = watchInitializationService.compile(null, new Script(ScriptType.INLINE, "painless", source, Collections.emptyMap()),
                scriptContext, validationErrors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.value(source);

        return builder;
    }

    public Factory getScriptFactory() {
        return scriptFactory;
    }

    public static <Factory> InlinePainlessScript<Factory> parse(String string, ScriptContext<Factory> scriptContext,
            WatchInitializationService watchInitializationService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        InlinePainlessScript<Factory> result = new InlinePainlessScript<Factory>(scriptContext, string);

        result.compile(watchInitializationService, validationErrors);
        validationErrors.throwExceptionForPresentErrors();
        return result;
    }

}
