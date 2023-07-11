package com.floragunn.signals.actions.watch.execute;

import java.io.IOException;
import java.util.Optional;

import com.floragunn.signals.watch.WatchInstanceIdService;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.floragunn.signals.execution.SimulationMode;

public class ExecuteWatchRequest extends ActionRequest {

    private String watchId;
    private String watchJson;
    private boolean recordExecution;
    private SimulationMode simulationMode;
    private String goTo;
    private String inputJson;
    private boolean includeAllRuntimeAttributesInResponse;

    public ExecuteWatchRequest() {
        super();
    }

    public ExecuteWatchRequest(String watchId, String watchJson, boolean recordExecution, SimulationMode simulationMode,
            boolean includeAllRuntimeAttributesInResponse) {
        super();
        this.watchId = watchId;
        this.watchJson = watchJson;
        this.recordExecution = recordExecution;
        this.simulationMode = simulationMode;
        this.includeAllRuntimeAttributesInResponse = includeAllRuntimeAttributesInResponse;
    }

    public ExecuteWatchRequest(StreamInput in) throws IOException {
        super(in);
        this.watchId = in.readOptionalString();
        this.watchJson = in.readOptionalString();
        this.recordExecution = in.readBoolean();
        this.simulationMode = in.readEnum(SimulationMode.class);
        this.goTo = in.readOptionalString();
        this.inputJson = in.readOptionalString();
        this.includeAllRuntimeAttributesInResponse = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(watchId);
        out.writeOptionalString(watchJson);
        out.writeBoolean(recordExecution);
        out.writeEnum(simulationMode);
        out.writeOptionalString(goTo);
        out.writeOptionalString(inputJson);
        out.writeBoolean(includeAllRuntimeAttributesInResponse);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getWatchId() {
        return watchId;
    }

    public String getWatchIdWithoutInstanceId() {
        return watchId == null ? null : WatchInstanceIdService.extractGenericWatchOrWatchId(watchId);
    }

    public Optional<String> getInstanceId() {
        return WatchInstanceIdService.extractInstanceId(watchId);
    }

    public void setWatchId(String watchId) {
        this.watchId = watchId;
    }

    public boolean isRecordExecution() {
        return recordExecution;
    }

    public void setRecordExecution(boolean recordExecution) {
        this.recordExecution = recordExecution;
    }

    public String getWatchJson() {
        return watchJson;
    }

    public void setWatchJson(String watchJson) {
        this.watchJson = watchJson;
    }

    public SimulationMode getSimulationMode() {
        return simulationMode;
    }

    public void setSimulationMode(SimulationMode simulationMode) {
        this.simulationMode = simulationMode;
    }

    public String getGoTo() {
        return goTo;
    }

    public void setGoTo(String goTo) {
        this.goTo = goTo;
    }

    public String getInputJson() {
        return inputJson;
    }

    public void setInputJson(String inputJson) {
        this.inputJson = inputJson;
    }

    public boolean isIncludeAllRuntimeAttributesInResponse() {
        return includeAllRuntimeAttributesInResponse;
    }

    public void setIncludeAllRuntimeAttributesInResponse(boolean includeAllRuntimeAttributesInResponse) {
        this.includeAllRuntimeAttributesInResponse = includeAllRuntimeAttributesInResponse;
    }

}
