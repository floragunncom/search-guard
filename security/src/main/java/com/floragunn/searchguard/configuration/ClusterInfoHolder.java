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

package com.floragunn.searchguard.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

public class ClusterInfoHolder implements ClusterStateListener {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private volatile DiscoveryNodes nodes = null;
    private volatile Boolean isLocalNodeElectedMaster = null;
    private volatile boolean initialized;
    
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if(nodes == null || event.nodesChanged()) {
            nodes = event.state().nodes();
            if(log.isDebugEnabled()) {
                log.debug("Cluster Info Holder now initialized for 'nodes'");
            }
            initialized = true;
        }
        
        isLocalNodeElectedMaster = event.localNodeMaster()?Boolean.TRUE:Boolean.FALSE;
    }

    public Boolean isLocalNodeElectedMaster() {
        return isLocalNodeElectedMaster;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Boolean hasNode(DiscoveryNode node) {
        if(nodes == null) {
            if(log.isDebugEnabled()) {
                log.debug("Cluster Info Holder not initialized yet for 'nodes'");
            }
            return null;
        }
        
        return nodes.nodeExists(node)?Boolean.TRUE:Boolean.FALSE;
    }
}
