/*
 * Copyright 2025 floragunn GmbH
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

package com.floragunn.searchsupport.jobs;

import com.floragunn.searchguard.SearchGuardModule;
import com.floragunn.searchsupport.jobs.actions.CheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.SchedulerConfigUpdateAction;
import com.floragunn.searchsupport.jobs.actions.TransportCheckForExecutingTriggerAction;
import com.floragunn.searchsupport.jobs.actions.TransportSchedulerConfigUpdateAction;
import org.elasticsearch.plugins.ActionPlugin;

import java.util.List;

import static com.floragunn.searchsupport.action.ActionHandlerFactory.actionHandler;

public class SchedulerModule implements SearchGuardModule {
    @Override
    public List<ActionPlugin.ActionHandler> getActions() {
        return List.of(
                actionHandler(CheckForExecutingTriggerAction.INSTANCE, TransportCheckForExecutingTriggerAction.class),
                actionHandler(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class)
        );
    }
}
