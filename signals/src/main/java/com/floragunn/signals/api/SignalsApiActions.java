/*
 * Copyright 2016-2019 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */
package com.floragunn.signals.api;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

public class SignalsApiActions {

    protected static final Logger log = LogManager.getLogger(SignalsApiActions.class);

    public static Collection<RestHandler> getHandler(Settings settings, Path configPath, RestController controller, Client client, ClusterService cs,
            ScriptService scriptService, NamedXContentRegistry xContentRegistry, ThreadPool threadPool) {
        return Arrays.asList(new WatchApiAction(settings, controller, threadPool),
                new ExecuteWatchApiAction(settings, controller, threadPool, client, scriptService, xContentRegistry),
                new DeActivateWatchAction(settings, controller), new AckWatchApiAction(settings, controller),
                new SearchWatchApiAction(settings, controller, threadPool), new AccountApiAction(settings, controller),
                new SearchAccountApiAction(settings, controller, threadPool), new WatchStateApiAction(settings, controller),
                new SettingsApiAction(settings, controller), new DeActivateTenantAction(settings, controller),
                new DeActivateGloballyAction(settings, controller), new SearchWatchStateApiAction(settings, controller, threadPool),
                new ConvertWatchApiAction(settings, controller, threadPool));
    }
}
