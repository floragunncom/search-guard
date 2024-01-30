/*
 * Copyright 2015-2017 floragunn GmbH
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

package org.elasticsearch.node;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PluginAwareNode extends Node {

    private static final AtomicBoolean loggingInitialized = new AtomicBoolean();

    private final boolean masterEligible;

    public PluginAwareNode(boolean masterEligible, final Settings preparedSettings) {
        this(masterEligible, preparedSettings, Collections.emptyList());
    }

    public PluginAwareNode(boolean masterEligible, final Settings preparedSettings, List<Class<? extends Plugin>> additionalPlugins) {
        super(NodeConstruction.prepareConstruction(
            createEnvironment(preparedSettings),
            new SgNodeServiceProvider(additionalPlugins),
            true));
        this.masterEligible = masterEligible;
    }

    private static Environment createEnvironment(Settings preparedSettings) {
        return configureESLogging(InternalSettingsPreparer.prepareEnvironment(preparedSettings,
            Collections.emptyMap(),
            null,
            () -> System.getenv("HOSTNAME")));
    }

    private static Environment configureESLogging(Environment environment) {
        if (!loggingInitialized.get()) {
            try {
                environment.configFile().toFile().mkdirs();
                byte[] log4jprops = Files.readAllBytes(Paths.get("src/test/resources/log4j2-test.properties"));
                Files.write(environment.configFile().resolve("log4j2.properties"), log4jprops);
                LogConfigurator.registerErrorListener();
                LogConfigurator.setNodeName("node");
                LogConfigurator.configure(environment, true);
                loggingInitialized.set(true);
                return environment;
            } catch(Exception e){
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else {
            return environment;
        }
    }

    public boolean isMasterEligible() {
        return masterEligible;
    }
}
