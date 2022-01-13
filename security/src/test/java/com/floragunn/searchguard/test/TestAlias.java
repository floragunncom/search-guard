package com.floragunn.searchguard.test;

import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.client.Client;
import org.junit.rules.ExternalResource;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.util.ImmutableSet;

public class TestAlias extends ExternalResource {

    private final String name;
    private final ImmutableSet<TestIndex> indices;
    private final LocalCluster cluster;

    public TestAlias(String name, LocalCluster cluster, TestIndex... indices) {
        this.name = name;
        this.cluster = cluster;
        this.indices = ImmutableSet.ofArray(indices);
    }

    @Override
    protected void before() throws Throwable {
        if (!cluster.isStarted()) {
            cluster.before();
        }
        
        try (Client client = cluster.getInternalNodeClient()) {
            
            for (TestIndex index : indices) {
                index.before();
            }
            
            client.admin().indices()
                    .aliases(new IndicesAliasesRequest().addAliasAction(AliasActions.add().indices(getIndexNamesAsArray()).alias(name))).actionGet();
        }
    }

    public String getName() {
        return name;
    }

    public ImmutableSet<TestIndex> getIndices() {
        return indices;
    }

    public String[] getIndexNamesAsArray() {
        return indices.stream().map(i -> i.getName()).collect(Collectors.toSet()).toArray(new String[0]);
    }
}