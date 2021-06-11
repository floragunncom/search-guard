package com.floragunn.signals.support;

import java.io.IOException;
import java.util.Collections;

import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;

import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.searchsupport.config.validation.ValueParser;
import com.floragunn.signals.watch.init.WatchInitializationService;

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

    public static class Parser<Factory> implements ValueParser<InlinePainlessScript<Factory>> {
        private final WatchInitializationService watchInitializationService;
        private final ScriptContext<Factory> scriptContext;

        public Parser(ScriptContext<Factory> scriptContext, WatchInitializationService watchInitializationService) {
            this.watchInitializationService = watchInitializationService;
            this.scriptContext = scriptContext;
        }

        @Override
        public InlinePainlessScript<Factory> parse(String string) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            InlinePainlessScript<Factory> result = new InlinePainlessScript<Factory>(scriptContext, string);

            result.compile(watchInitializationService, validationErrors);
            validationErrors.throwExceptionForPresentErrors();
            return result;
        }

        @Override
        public String getExpectedValue() {
            return "Painless script";
        }
    }

}
