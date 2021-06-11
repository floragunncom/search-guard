package com.floragunn.signals.script;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;

import com.floragunn.signals.SignalsModule;

public class SignalsPainlessExtension implements PainlessExtension {
    private final static Logger log = LogManager.getLogger(SignalsPainlessExtension.class);
    private final static SignalsModule MODULE = new SignalsModule();
    
    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {

        Whitelist whitelist = WhitelistLoader.loadFromResourceFiles(SignalsPainlessExtension.class, "SignalsPainlessClassWhitelist.txt");

        log.info("Loaded script whitelist: " + whitelist);

        HashMap<ScriptContext<?>, List<Whitelist>> result = new HashMap<>();

        for (ScriptContext<?> context : MODULE.getContexts()) {
            result.put(context, Collections.singletonList(whitelist));
        }

        return result;
    }

}
