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

package com.floragunn.searchguard.ssl.test.helper.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.mustache.MustachePlugin;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.google.common.collect.Lists;
import org.elasticsearch.transport.netty4.Netty4Plugin;

public enum ClusterConfiguration {
    //first one needs to be a master
    //HUGE(new NodeSettings(true, false, false), new NodeSettings(true, false, false), new NodeSettings(true, false, false), new NodeSettings(false, true,false), new NodeSettings(false, true, false)),

    //3 nodes (1m, 2d)
    DEFAULT(new NodeSettings(true, false), new NodeSettings(false, true), new NodeSettings(false, true)),

    //1 node (1md)
    SINGLENODE(new NodeSettings(true, true)),

    //4 node (1m, 2d, 1c)
    CLIENTNODE(new NodeSettings(true, false), new NodeSettings(false, true), new NodeSettings(false, true), new NodeSettings(false, false)),

    THREE_MASTERS(new NodeSettings(true, false), new NodeSettings(true, false), new NodeSettings(true, false), new NodeSettings(false, true), new NodeSettings(false, true));

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
        public boolean masterNode;
        public boolean dataNode;
        public List<Class<? extends Plugin>> plugins = Lists.newArrayList(Netty4Plugin.class, SearchGuardSSLPlugin.class,
                MustachePlugin.class, ParentJoinPlugin.class, PercolatorPlugin.class, ReindexPlugin.class);

        public NodeSettings(boolean masterNode, boolean dataNode) {
            super();
            this.masterNode = masterNode;
            this.dataNode = dataNode;

            tryToIncludePainless();
        }

        public NodeSettings(boolean masterNode, boolean dataNode, List<Class<? extends Plugin>> additionalPlugins) {
            this(masterNode, dataNode);

            this.plugins.addAll(additionalPlugins);
        }

        @SuppressWarnings("unchecked")
        public Class<? extends Plugin>[] getPlugins() {
            return plugins.toArray(new Class[0]);
        }
        
        @SuppressWarnings("unchecked")
        public Class<? extends Plugin>[] getPlugins(List<Class<? extends Plugin>> additionalPlugins) {
            List<Class<? extends Plugin>> plugins = new ArrayList<>(this.plugins);
            
            if (additionalPlugins != null) {
                plugins.addAll(additionalPlugins);
            }
            
            return plugins.toArray(new Class[0]);
        }

        private void tryToIncludePainless() {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Plugin> painlessPlugin = (Class<? extends Plugin>) Class.forName("org.elasticsearch.painless.PainlessPlugin");

                plugins.add(painlessPlugin);

            } catch (ClassNotFoundException e) {

            }
        }
    }
}
