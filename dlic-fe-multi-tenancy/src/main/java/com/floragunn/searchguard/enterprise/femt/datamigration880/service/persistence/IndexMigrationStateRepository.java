/*
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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service.persistence;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationExecutionSummary;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.IndexAlreadyExistsException;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.MigrationStateRepository;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.OptimisticLock;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.OptimisticLockException;
import com.floragunn.searchguard.enterprise.femt.datamigration880.service.RepositoryException;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.StaticSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.common.Strings.requireNonEmpty;

public class IndexMigrationStateRepository implements MigrationStateRepository {

    final static String INDEX_NAME = ".sg_data_migration_state";

    private static final Logger log = LogManager.getLogger(IndexMigrationStateRepository.class);

    private final PrivilegedConfigClient client;

    public IndexMigrationStateRepository(PrivilegedConfigClient client) {
        this.client = Objects.requireNonNull(client, "Privileged client is required");
    }

    private void createOrUpdate(String id, MigrationExecutionSummary migrationExecutionSummary, boolean creationRequired) {
        Objects.requireNonNull(id, "Data migration summary id is required");
        Objects.requireNonNull(migrationExecutionSummary, "Cannot persist null data migration summary");
        IndexRequest request = new IndexRequest(INDEX_NAME) //
            .setRefreshPolicy(IMMEDIATE) //
            .id(id) //
            .create(creationRequired) //
            .source(migrationExecutionSummary.toJsonString(), XContentType.JSON);
        DocWriteResponse response = client.index(request).actionGet();
        throwOnFailure(response.status(), "Cannot store or update migration status");
    }

    @Override
    public void create(String id, MigrationExecutionSummary summary) throws OptimisticLockException {
        log.debug("Creating migration data '{}' with id '{}'.", summary, id);
        try {
            createOrUpdate(id, summary, true);
        } catch (VersionConflictEngineException e) {
            String message = String.format("Migration document with id '%s' already exists", id);
            throw new OptimisticLockException(message, e);
        }
    }

    @Override
    public void upsert(String id, MigrationExecutionSummary migrationExecutionSummary) {
        log.debug("Upsertting migration data '{}' using id '{}'", migrationExecutionSummary, id);
        createOrUpdate(id, migrationExecutionSummary, false);
    }

    @Override
    public void updateWithLock(String id, MigrationExecutionSummary migrationExecutionSummary, OptimisticLock lock)
        throws OptimisticLockException {
        log.debug("Update migration data '{}' using id '{}' with lockData '{}'", migrationExecutionSummary, id, lock);
        Objects.requireNonNull(id, "Data migration summary id is required");
        Objects.requireNonNull(migrationExecutionSummary, "Cannot persist null data migration summary");
        Objects.requireNonNull(lock, "Optimistic lockData data are required to save data migration summary");
        IndexRequest request = new IndexRequest(INDEX_NAME) //
            .setRefreshPolicy(IMMEDIATE) //
            .id(id) //
            .source(migrationExecutionSummary.toJsonString(), XContentType.JSON) //
            .setIfSeqNo(lock.seqNo()) //
            .setIfPrimaryTerm(lock.primaryTerm());
        try {
            DocWriteResponse response = client.index(request).actionGet();
            throwOnFailure(response.status(), "Cannot update migration status with lock.");
        } catch (VersionConflictEngineException e) {
            String message = String.format("Optimistic lock failure for data migration document '%s' and lock data '%s'.", id, lock);
            throw new OptimisticLockException(message, e);
        }
    }

    @Override
    public boolean isIndexCreated() {
        GetIndexRequest request = new GetIndexRequest(StaticSettings.DEFAULT_MASTER_TIMEOUT).indices("*");
        GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();
        return Arrays.asList(response.indices()).contains(INDEX_NAME);
    }

    @Override
    public void createIndex() throws IndexAlreadyExistsException {
        Settings settings = Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 0).put(SETTING_AUTO_EXPAND_REPLICAS, "0-1").build();
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME).mapping(MigrationExecutionSummary.MAPPING).settings(settings);
        try {
            CreateIndexResponse response = client.admin().indices().create(request).actionGet();
            throwOnFailure(response, "Cannot create index '" + INDEX_NAME + "'");
        } catch (ResourceAlreadyExistsException e) {
            throw new IndexAlreadyExistsException("Index " + INDEX_NAME + " already exists.", e);
        }
    }

    @Override
    public Optional<MigrationExecutionSummary> findById(String id) {
        requireNonEmpty(id, "Data migration state document id is required");
        try {
            GetResponse response = client.get(new GetRequest(INDEX_NAME, id)).actionGet();
            if(response.isExists()){
                return Optional.of(response).map(this::parseMigrationExecutionSummary);
            }
            return Optional.empty();
        } catch (IndexNotFoundException indexNotFoundException) {
            return Optional.empty();
        }
    }

    private MigrationExecutionSummary parseMigrationExecutionSummary(GetResponse response) {
        try {
            DocNode docNode = DocNode.parse(Format.JSON).from(response.getSourceAsString());
            long primaryTerm = response.getPrimaryTerm();
            long seqNo = response.getSeqNo();
            return MigrationExecutionSummary.parse(docNode, primaryTerm, seqNo);
        } catch (DocumentParseException e) {
            throw new RuntimeException("Cannot parse frontend migration state document", e);
        }
    }

    private boolean isSuccess(RestStatus restStatus) {
        return (restStatus.getStatus() >= 200) && (restStatus.getStatus() < 300);
    }

    private boolean isFailure(RestStatus restStatus) {
        return ! isSuccess(restStatus);
    }

    private void throwOnFailure(RestStatus status, String message) {
        if(isFailure(status)) {
            throw new RepositoryException(message, status);
        }
    }

    private void throwOnFailure(AcknowledgedResponse response, String message) {
        if(!response.isAcknowledged()) {
            throw new RepositoryException(message, RestStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
