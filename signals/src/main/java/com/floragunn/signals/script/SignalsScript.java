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

import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.watch.severity.SeverityMapping;
import java.util.Map;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

public abstract class SignalsScript {

    private final Map<String, Object> params;
    private final Map<String, Object> data;
    private final WatchExecutionContextData.TriggerInfo trigger;
    private final WatchExecutionContextData.WatchInfo watch;
    private final Map<String, Object> item;
    private final SeverityMapping.EvaluationResult severity;
    private final JodaCompatibleZonedDateTime execution_time;
    private final WatchExecutionContextData resolved;

    protected SignalsScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
        this.params = params;
        this.data = watchRuntimeContext.getContextData().getData();
        this.trigger = watchRuntimeContext.getContextData().getTriggerInfo();
        this.execution_time = watchRuntimeContext.getContextData().getExecutionTime();

        this.severity = watchRuntimeContext.getContextData().getSeverity();

        this.resolved = watchRuntimeContext.getResolvedContextData();
        this.item = watchRuntimeContext.getContextData().getItem();
        this.watch = watchRuntimeContext.getContextData().getWatch();
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public WatchExecutionContextData.TriggerInfo getTrigger() {
        return trigger;
    }

    public JodaCompatibleZonedDateTime getExecution_time() {
        return execution_time;
    }

    public SeverityMapping.EvaluationResult getSeverity() {
        return severity;
    }

    public WatchExecutionContextData getResolved() {
        return resolved;
    }

    public Map<String, Object> getItem() {
        return item;
    }

    public WatchExecutionContextData.WatchInfo getWatch() {
        return watch;
    }
}
