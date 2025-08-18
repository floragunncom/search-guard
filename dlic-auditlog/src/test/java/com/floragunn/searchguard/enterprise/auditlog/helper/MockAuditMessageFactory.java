/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.auditlog.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;

import com.floragunn.searchsupport.util.EsLogging;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;

import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.Category;

public class MockAuditMessageFactory {
    static {
        EsLogging.initLogging();
    }

    public static AuditMessage validAuditMessage() {
        return validAuditMessage(Category.FAILED_LOGIN);
    }

    public static AuditMessage validAuditMessage(Category category) {

        ClusterState cs = mock(ClusterState.class);
        DiscoveryNodes dns = mock(DiscoveryNodes.class);
        DiscoveryNode dn = mock(DiscoveryNode.class);

        when(dn.getHostAddress()).thenReturn("hostaddress");
        when(dn.getId()).thenReturn("hostaddress");
        when(dn.getHostName()).thenReturn("hostaddress");
        when(dns.getLocalNode()).thenReturn(dn);
        when(cs.nodes()).thenReturn(dns);
        when(cs.getClusterName()).thenReturn(new ClusterName("testcluster"));

        TransportAddress ta = new TransportAddress(new InetSocketAddress("8.8.8.8", 80));

        AuditMessage msg = new AuditMessage(category, cs, Origin.TRANSPORT, Origin.TRANSPORT);
        msg.addEffectiveUser("John Doe");
        msg.addRemoteAddress(ta);
        msg.addRequestType("IndexRequest");
        return msg;
    }

}
