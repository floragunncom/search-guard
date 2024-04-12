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

package com.floragunn.searchguard.action.licenseinfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.license.LicenseRepository;
import com.floragunn.searchguard.license.SearchGuardLicense;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.ModuleInfo;
import com.floragunn.searchguard.support.ModuleType;

@Deprecated
public class TransportLicenseInfoAction extends
        TransportNodesAction<LicenseInfoRequest, LicenseInfoResponse, TransportLicenseInfoAction.NodeLicenseRequest, LicenseInfoNodeResponse> {

    private final LicenseRepository licenseRepository;
    private final Settings settings;

    @Inject
    public TransportLicenseInfoAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final LicenseRepository licenseRepository, final ActionFilters actionFilters) {

        super(LicenseInfoAction.NAME, clusterService, transportService, actionFilters,
                TransportLicenseInfoAction.NodeLicenseRequest::new, threadPool.executor(ThreadPool.Names.MANAGEMENT));

        this.licenseRepository = licenseRepository;
        this.settings = settings;
    }

    public static class NodeLicenseRequest extends BaseNodesRequest {

        LicenseInfoRequest request;

        public NodeLicenseRequest(final LicenseInfoRequest request) {
            super((String[]) null);
            this.request = request;
        }

        public NodeLicenseRequest(StreamInput in) throws IOException {
            super(in);
            this.request = new LicenseInfoRequest(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            this.request.writeTo(out);
        }
    }

    @Override
    protected LicenseInfoNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new LicenseInfoNodeResponse(in);
    }

    @Override
    protected LicenseInfoResponse newResponse(LicenseInfoRequest request, List<LicenseInfoNodeResponse> responses,
            List<FailedNodeException> failures) {
        return new LicenseInfoResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected LicenseInfoNodeResponse nodeOperation(final NodeLicenseRequest request, Task task) {
        final SearchGuardLicense license = licenseRepository.getLicense();

        Set<ModuleInfo> moduleInfo = new HashSet<>();

        if (settings.getAsBoolean(ConfigConstants.SEARCHGUARD_ENTERPRISE_MODULES_ENABLED, true)) {
            // This serves as a kind of capability info for the Kibana plugin, so we need to provide this information to keep older versions happy

            moduleInfo.add(new ModuleInfo(ModuleType.REST_MANAGEMENT_API, "n/a"));
            moduleInfo.add(new ModuleInfo(ModuleType.DLSFLS, "n/a"));
            moduleInfo.add(new ModuleInfo(ModuleType.MULTITENANCY, "n/a"));
        }

        return new LicenseInfoNodeResponse(clusterService.localNode(), license, moduleInfo);
    }

    @Override
    protected NodeLicenseRequest newNodeRequest(LicenseInfoRequest request) {
        return new NodeLicenseRequest(request);
    }
}
