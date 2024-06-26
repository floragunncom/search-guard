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

package com.floragunn.searchguard.test.helper.cluster;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.transport.TransportAddress;

public class ClusterInfo {
	public int numNodes;
	public String httpHost = null;
	public int httpPort = -1;
	public List<TransportAddress> httpAdresses = new ArrayList<TransportAddress>();
	public String nodeHost;
	public int nodePort;
	public String clustername;
    public List<String> tcpMasterPortsOnly;
    
    @Override
    public String toString() {
        return "ClusterInfo [numNodes=" + numNodes + ", httpHost=" + httpHost + ", httpPort=" + httpPort + ", httpAdresses=" + httpAdresses
                + ", nodeHost=" + nodeHost + ", nodePort=" + nodePort + ", clustername=" + clustername + ", tcpMasterPortsOnly=" + tcpMasterPortsOnly
                + "]";
    }
}
