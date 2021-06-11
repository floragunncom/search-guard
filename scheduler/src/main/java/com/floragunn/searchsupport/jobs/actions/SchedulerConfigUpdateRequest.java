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

package com.floragunn.searchsupport.jobs.actions;

import java.io.IOException;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class SchedulerConfigUpdateRequest extends BaseNodesRequest<SchedulerConfigUpdateRequest> {

    private String schedulerName;
    //private Collection<String> addedJobs;
    // private Collection<String> modifiedJobs;
    //private Collection<String> deletedJobs;

    public SchedulerConfigUpdateRequest(StreamInput in) throws IOException {
        super(in);
        this.schedulerName = in.readString();

    }

    public SchedulerConfigUpdateRequest(String schedulerName) {
        super(new String[] {});
        this.schedulerName = schedulerName;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(schedulerName);
        //  out.writeStringCollection(this.addedJobs);
        // out.writeStringCollection(this.modifiedJobs);
        // out.writeStringCollection(this.deletedJobs);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (schedulerName == null || schedulerName.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    public void setSchedulerName(String schedulerName) {
        this.schedulerName = schedulerName;
    }

}
