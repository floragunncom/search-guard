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
package com.floragunn.signals.actions.watch.execute;

import com.floragunn.signals.execution.SimulationMode;
import java.io.IOException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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
