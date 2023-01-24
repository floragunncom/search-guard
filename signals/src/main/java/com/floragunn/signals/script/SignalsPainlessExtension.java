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
package com.floragunn.signals.script;

import com.floragunn.signals.SignalsModule;
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
