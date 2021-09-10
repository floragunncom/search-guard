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
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SearchGuardLicense;
import com.floragunn.searchguard.support.ReflectionHelper;

public class TransportLicenseInfoAction
extends
TransportNodesAction<LicenseInfoRequest, LicenseInfoResponse, TransportLicenseInfoAction.NodeLicenseRequest, LicenseInfoNodeResponse> {

    private final ConfigurationRepository configurationRepository;
    
    @Inject
    public TransportLicenseInfoAction(final Settings settings,
            final ThreadPool threadPool, final ClusterService clusterService, final TransportService transportService,
            final ConfigurationRepository configurationRepository, final ActionFilters actionFilters) {
        
        super(LicenseInfoAction.NAME, threadPool, clusterService, transportService, actionFilters,
                LicenseInfoRequest::new, TransportLicenseInfoAction.NodeLicenseRequest::new,
                ThreadPool.Names.MANAGEMENT, LicenseInfoNodeResponse.class);

        this.configurationRepository = configurationRepository;
    }

    public static class NodeLicenseRequest extends BaseNodeRequest {

        LicenseInfoRequest request;

        public NodeLicenseRequest(final LicenseInfoRequest request) {
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
    protected LicenseInfoNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LicenseInfoNodeResponse(in);
    }
    
    @Override
    protected LicenseInfoResponse newResponse(LicenseInfoRequest request, List<LicenseInfoNodeResponse> responses,
            List<FailedNodeException> failures) {
        return new LicenseInfoResponse(this.clusterService.getClusterName(), responses, failures);

    }
	
    @Override
    protected LicenseInfoNodeResponse nodeOperation(final NodeLicenseRequest request) {
        final SearchGuardLicense license = configurationRepository.getLicense();
        return new LicenseInfoNodeResponse(clusterService.localNode(), license, ReflectionHelper.getModulesLoaded()); 
    }

    @Override
    protected NodeLicenseRequest newNodeRequest(LicenseInfoRequest request) {
        return new NodeLicenseRequest(request);
    }
}
