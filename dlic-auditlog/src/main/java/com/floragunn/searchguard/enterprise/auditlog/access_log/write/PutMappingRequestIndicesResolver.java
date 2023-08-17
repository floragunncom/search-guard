/*
 * Based on https://github.com/elastic/elasticsearch/blob/v7.10.2/server/src/main/java/org/elasticsearch/action/admin/indices/mapping/put/TransportPutMappingAction.java
 * from Apache 2 licensed Elasticsearch 7.10.2.
 *
 * Original license header:
 *
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *
 *
 * Modifications:
 *
 * Copyright 2023 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.access_log.write;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class PutMappingRequestIndicesResolver {

    static Set<String> resolveIndexNames(PutMappingRequest request, IndexNameExpressionResolver indexNameExpressionResolver, ClusterState clusterState) {
        if (request.getConcreteIndex() != null) {
            return new HashSet<>(Collections.singletonList(request.getConcreteIndex().getName()));
        } else {
            if (request.writeIndexOnly()) {
                return Stream.of(request.indices()).map(
                        indexName -> indexNameExpressionResolver.concreteWriteIndex(
                                clusterState, request.indicesOptions(), indexName, request.indicesOptions().allowNoIndices(),
                                request.includeDataStreams()
                        ).getName()
                ).collect(Collectors.toSet());
            } else {
                return new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndexNames(clusterState, request)));
            }
        }
    }

}
