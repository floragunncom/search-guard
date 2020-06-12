package com.floragunn.signals.confconv.es;

import com.floragunn.searchsupport.config.validation.ValidationError;
import com.floragunn.searchsupport.config.validation.ValidationErrors;
import com.floragunn.signals.confconv.ConversionResult;

public class MustacheTemplateConverter {
    private final String script;

    MustacheTemplateConverter(String script) {
        this.script = script;
    }

    public ConversionResult<String> convertToSignals() {
        if (script == null) {
            return new ConversionResult<String>(null);
        }
        
        ValidationErrors validationErrors = new ValidationErrors();
        
        StringBuilder result = new StringBuilder();
        
        for (int i = 0; i < script.length(); ) {
            int expressionStart = script.indexOf("{{", i);
            
            if (expressionStart == -1) {
                result.append(script.substring(i));
                break;
            }

            int expressionEnd = script.indexOf("}}", expressionStart + 1);
            
            if (expressionEnd == -1) {
                result.append(script.substring(i));
                break;
            }

            result.append(script.substring(i, expressionStart));
            result.append("{{");
            
            String expression = script.substring(expressionStart + 2, expressionEnd);
            String convertedExpression = expression;
            
            if (expression.contains("ctx.payload.")) {
                convertedExpression = convertedExpression.replace("ctx.payload.", "data.");
            } else if (expression.contains("params.")) {
                validationErrors.add(new ValidationError(null, "params script attribute is not supported by Signals"));
            } else if (expression.contains("ctx.metadata.")) {
                convertedExpression = convertedExpression.replace("ctx.metadata.", "data.");
            } else if (expression.contains("ctx.trigger.")) {
                convertedExpression = convertedExpression.replace("ctx.trigger.", "trigger.");
            } 
            
            result.append(convertedExpression);
            result.append("}}");
            
            i = expressionEnd + 2;
        }
                
        return new ConversionResult<String>(result.toString());
    }
    
    

}
