package com.floragunn.signals.script;

import java.time.ZonedDateTime;
import java.util.Map;

import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.execution.WatchExecutionContextData;
import com.floragunn.signals.watch.severity.SeverityMapping;

public abstract class SignalsScript {

    private final Map<String, Object> params;
    private final Map<String, Object> data;
    private final WatchExecutionContextData.TriggerInfo trigger;
    private final WatchExecutionContextData.WatchInfo watch;
    private final Map<String, Object> item;
    private final SeverityMapping.EvaluationResult severity;
    private final ZonedDateTime execution_time;
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

    public ZonedDateTime getExecution_time() {
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