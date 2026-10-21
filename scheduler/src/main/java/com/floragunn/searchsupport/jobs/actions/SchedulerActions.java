/*
 * Copyright 2020-2025 floragunn GmbH
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

package com.floragunn.searchsupport.jobs.actions;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

import static com.floragunn.searchsupport.action.ActionHandlerFactory.actionHandler;

public class SchedulerActions {
    public static List<ActionHandler> getActions() {
        return Arrays.asList(actionHandler(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class),
                actionHandler(CheckForExecutingTriggerAction.INSTANCE, TransportCheckForExecutingTriggerAction.class));
    }
}
