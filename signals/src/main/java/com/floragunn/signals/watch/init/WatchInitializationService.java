package com.floragunn.signals.watch.init;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptException;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.TemplateScript;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.signals.accounts.Account;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.accounts.NoSuchAccountException;
import com.floragunn.signals.support.ScriptValidationError;

public class WatchInitializationService {
    private final static Logger log = LogManager.getLogger(WatchInitializationService.class);

    private final ScriptService scriptService;
    private final AccountRegistry accountRegistry;

    public WatchInitializationService(AccountRegistry accountRegistry, ScriptService scriptService) {
        this.accountRegistry = accountRegistry;
        this.scriptService = scriptService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public boolean isScriptCompilationRequired() {
        return scriptService != null;
    }

    public TemplateScript.Factory compileTemplate(String attribute, String scriptSource, ValidationErrors validationErrors) {
        return compile(attribute, scriptSource, Script.DEFAULT_TEMPLATE_LANG, TemplateScript.CONTEXT, validationErrors);
    }

    public List<TemplateScript.Factory> compileTemplates(String attribute, String[] scriptSourceArray, ValidationErrors validationErrors) {
        if (scriptSourceArray == null || scriptSourceArray.length == 0) {
            return Collections.emptyList();
        }

        ArrayList<TemplateScript.Factory> result = new ArrayList<>(scriptSourceArray.length);

        int i = 0;
        for (String scriptSource : scriptSourceArray) {

            result.add(compileTemplate(attribute + "[" + i + "]", scriptSource, validationErrors));

            i++;
        }

        return result;
    }

    public List<TemplateScript.Factory> compileTemplates(String attribute, List<String> scriptSources, ValidationErrors validationErrors) {
        if (scriptSources == null) {
            return null;
        }

        if (scriptSources.size() == 0) {
            return Collections.emptyList();
        }

        ArrayList<TemplateScript.Factory> result = new ArrayList<>(scriptSources.size());

        int i = 0;
        for (String scriptSource : scriptSources) {

            result.add(compileTemplate(attribute + "[" + i + "]", scriptSource, validationErrors));

            i++;
        }

        return result;
    }

    public <FactoryType> FactoryType compile(String attribute, String scriptSource, String language, ScriptContext<FactoryType> context,
            ValidationErrors validationErrors) {
        if (scriptSource == null) {
            return null;
        }

        return compile(attribute, new Script(ScriptType.INLINE, language, scriptSource, Collections.emptyMap()), context, validationErrors);
    }

    public <FactoryType> FactoryType compile(String attribute, Script script, ScriptContext<FactoryType> context, ValidationErrors validationErrors) {
        if (scriptService == null) {
            return null;
        }

        try {
            return scriptService.compile(script, context);
        } catch (ScriptException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while compiling script " + script, e);
            }

            validationErrors.add(new ScriptValidationError(attribute, e));
            return null;
        }
    }

    public <T extends Account> boolean verifyAccount(String id, Class<T> accountType, ValidationErrors validationErrors, DocNode jsonObject) {
        if (accountRegistry == null) {
            return true;
        }

        try {
            accountRegistry.lookupAccount(id, accountType);

            return true;
        } catch (NoSuchAccountException e) {
            validationErrors.add(new ValidationError("account", e.getMessage(), jsonObject).cause(e));

            return false;
        }
    }

    public AccountRegistry getAccountRegistry() {
        return accountRegistry;
    }
}
