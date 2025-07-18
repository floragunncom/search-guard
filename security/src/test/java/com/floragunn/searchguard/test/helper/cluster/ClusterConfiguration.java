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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public enum ClusterConfiguration {
    //first one needs to be a master
    //HUGE(new NodeSettings(true, false, false), new NodeSettings(true, false, false), new NodeSettings(true, false, false), new NodeSettings(false, true,false), new NodeSettings(false, true, false)),

    //3 nodes (1m, 2d)
    DEFAULT(new NodeSettings("master", true, false), new NodeSettings("data_1", false, true), new NodeSettings("data_2", false, true)),

    //1 node (1md)
    SINGLENODE(new NodeSettings("single", true, true)),

    //4 node (1m, 2d, 1c)
    CLIENTNODE(new NodeSettings("master", true, false), new NodeSettings("data_1", false, true), new NodeSettings("data_2", false, true), new NodeSettings("client_1", false, false)),

    THREE_MASTERS(new NodeSettings("master_1", true, false), new NodeSettings("master_2", true, false), new NodeSettings("master_3", true, false),
            new NodeSettings("data_1", false, true), new NodeSettings("data_2", false, true));

    private List<NodeSettings> nodeSettings = new LinkedList<>();

    private ClusterConfiguration(NodeSettings... settings) {
        nodeSettings.addAll(Arrays.asList(settings));
    }

    public List<NodeSettings> getNodeSettings() {
        return Collections.unmodifiableList(nodeSettings);
    }

    public List<NodeSettings> getMasterNodeSettings() {
        return Collections.unmodifiableList(nodeSettings.stream().filter(a -> a.masterNode).collect(Collectors.toList()));
    }

    public List<NodeSettings> getNonMasterNodeSettings() {
        return Collections.unmodifiableList(nodeSettings.stream().filter(a -> !a.masterNode).collect(Collectors.toList()));
    }

    public int getNodes() {
        return nodeSettings.size();
    }

    public int getMasterNodes() {
        return (int) nodeSettings.stream().filter(a -> a.masterNode).count();
    }

    public int getDataNodes() {
        return (int) nodeSettings.stream().filter(a -> a.dataNode).count();
    }

    public int getClientNodes() {
        return (int) nodeSettings.stream().filter(a -> !a.masterNode && !a.dataNode).count();
    }

    public static class NodeSettings {
        public final boolean masterNode;
        public final boolean dataNode;
        public final String name;

        public NodeSettings(String name, boolean masterNode, boolean dataNode) {
            this.name = name;
            this.masterNode = masterNode;
            this.dataNode = dataNode;
        }
    }
}
