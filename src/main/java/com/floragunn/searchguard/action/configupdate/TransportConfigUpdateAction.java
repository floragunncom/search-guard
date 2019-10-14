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

package com.floragunn.searchguard.action.configupdate;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.floragunn.searchguard.auth.BackendRegistry;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SearchGuardLicense;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.support.LicenseHelper;

public class TransportConfigUpdateAction
extends
TransportNodesAction<ConfigUpdateRequest, ConfigUpdateResponse, TransportConfigUpdateAction.NodeConfigUpdateRequest, ConfigUpdateNodeResponse> {

    protected Logger logger = LogManager.getLogger(getClass());
    private final Provider<BackendRegistry> backendRegistry;
    private final ConfigurationRepository configurationRepository;
    private DynamicConfigFactory dynamicConfigFactory;
    
    @Inject
    public TransportConfigUpdateAction(final Settings settings,
            final ThreadPool threadPool, final ClusterService clusterService, final TransportService transportService,
            final ConfigurationRepository configurationRepository, final ActionFilters actionFilters,
            Provider<BackendRegistry> backendRegistry, DynamicConfigFactory dynamicConfigFactory) {        
        super(ConfigUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters,
                ConfigUpdateRequest::new, TransportConfigUpdateAction.NodeConfigUpdateRequest::new,
                ThreadPool.Names.MANAGEMENT, ConfigUpdateNodeResponse.class);

        this.configurationRepository = configurationRepository;
        this.backendRegistry = backendRegistry;
        this.dynamicConfigFactory = dynamicConfigFactory;
    }

    public static class NodeConfigUpdateRequest extends BaseNodeRequest {

        ConfigUpdateRequest request;
        
        public NodeConfigUpdateRequest(StreamInput in) throws IOException {
            super(in);
            request = new ConfigUpdateRequest(in);
        }

        public NodeConfigUpdateRequest(final ConfigUpdateRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    @Override
    protected ConfigUpdateNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ConfigUpdateNodeResponse(in);
    }
    
    @Override
    protected ConfigUpdateResponse newResponse(ConfigUpdateRequest request, List<ConfigUpdateNodeResponse> responses,
            List<FailedNodeException> failures) {
        return new ConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected ConfigUpdateNodeResponse nodeOperation(final NodeConfigUpdateRequest request) {
        configurationRepository.reloadConfiguration(CType.fromStringValues((request.request.getConfigTypes())));
        backendRegistry.get().invalidateCache();
        
        /*final SearchGuardLicense license = configurationRepository.getLicense(); 
        
        if (license != null) {
            if(!license.isValid()) {
                logger.warn("License "+license.getUid()+" is invalid due to "+license.getMsgs());
                //throw an exception here if loading of invalid license should be denied
            }
        }*/
        
        final String licenseText = dynamicConfigFactory.getLicenseString();
        
        if(licenseText != null && !licenseText.isEmpty()) {
            try {
                final SearchGuardLicense license = new SearchGuardLicense(XContentHelper.convertToMap(XContentType.JSON.xContent(), LicenseHelper.validateLicense(licenseText), true), clusterService);
                
                if(!license.isValid()) {
                    logger.warn("License "+license.getUid()+" is invalid due to "+license.getMsgs());
                    //throw an exception here if loading of invalid license should be denied
                }
            } catch (Exception e) {
                logger.error("Invalid license",e);
                return new ConfigUpdateNodeResponse(clusterService.localNode(), new String[0], "Invalid license: "+e); 
            }
        }

        
        return new ConfigUpdateNodeResponse(clusterService.localNode(), request.request.getConfigTypes(), null); 
    }

    @Override
    protected NodeConfigUpdateRequest newNodeRequest(ConfigUpdateRequest request) {
        return new NodeConfigUpdateRequest(request);
    }
}
