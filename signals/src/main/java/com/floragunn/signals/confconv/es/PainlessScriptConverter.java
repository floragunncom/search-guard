package com.floragunn.signals.confconv.es;

import com.floragunn.searchsupport.jobs.config.validation.ValidationError;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.signals.confconv.ConversionResult;

public class PainlessScriptConverter {
    private final String script;

    PainlessScriptConverter(String script) {
        this.script = script;
    }

    public ConversionResult<String> convertToSignals() {
        ValidationErrors validationErrors = new ValidationErrors();
        
        String convertedScript = script;

        if (script.contains("ctx.payload.")) {
            convertedScript = convertedScript.replace("ctx.payload.", "data.");
        }
        
        if (script.contains("params.")) {
            validationErrors.add(new ValidationError(null, "params script attribute is not supported by Signals"));
        }
        
        if (script.contains("ctx.metadata.")) {
            convertedScript = convertedScript.replace("ctx.metadata.", "data.");
        }
        
        return new ConversionResult<String>(convertedScript);
    }

}
