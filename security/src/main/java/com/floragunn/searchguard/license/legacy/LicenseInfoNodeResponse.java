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

package com.floragunn.searchguard.license.legacy;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.floragunn.searchguard.license.SearchGuardLicense;

@Deprecated
public class LicenseInfoNodeResponse extends BaseNodeResponse {

    private SearchGuardLicense license;
    private Set<LicenseInfoResponse.ModuleInfo> modules;

    public LicenseInfoNodeResponse(StreamInput in) throws IOException {
        super(in);
        license = in.readOptionalWriteable(SearchGuardLicense::new);
        modules = new HashSet<>(in.readCollectionAsList(LicenseInfoResponse.ModuleInfo::new));
    }

    public LicenseInfoNodeResponse(final DiscoveryNode node, SearchGuardLicense license, Set<LicenseInfoResponse.ModuleInfo> modules) {
        super(node);
        this.license = license;
        this.modules = modules;
    }

    public static LicenseInfoNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new LicenseInfoNodeResponse(in);
    }

    public SearchGuardLicense getLicense() {
        return license;
    }

    public Set<LicenseInfoResponse.ModuleInfo> getModules() {
        return modules;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(license);
        out.writeCollection(new LinkedList<>(modules));
    }

    @Override
    public String toString() {
        return "LicenseInfoNodeResponse [license=" + license + "]";
    }
}
