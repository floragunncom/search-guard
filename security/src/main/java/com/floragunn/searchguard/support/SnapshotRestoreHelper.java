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
package com.floragunn.searchguard.support;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotUtils;
import org.elasticsearch.threadpool.ThreadPool;

public class SnapshotRestoreHelper {

    protected static final Logger log = LogManager.getLogger(SnapshotRestoreHelper.class);

    private final static String GENERC_THREAD_NAME = "[" + ThreadPool.Names.GENERIC + "]";

    public static List<String> resolveOriginalIndices(RestoreSnapshotRequest restoreRequest, RepositoriesService repositoriesService) {
        final SnapshotInfo snapshotInfo = getSnapshotInfo(restoreRequest, repositoriesService);

        if (snapshotInfo == null) {
            log.warn("snapshot repository '" + restoreRequest.repository() + "', snapshot '" + restoreRequest.snapshot() + "' not found");
            return null;
        } else {
            return SnapshotUtils.filterIndices(snapshotInfo.indices(), restoreRequest.indices(), restoreRequest.indicesOptions());
        }

    }

    public static SnapshotInfo getSnapshotInfo(RestoreSnapshotRequest restoreRequest, RepositoriesService repositoriesService) {
        final Repository repository = repositoriesService.repository(restoreRequest.repository());
        final String threadName = Thread.currentThread().getName();
        SnapshotInfo snapshotInfo = null;

        try {
            setCurrentThreadName(GENERC_THREAD_NAME);

            final RepositoryDataListener repositoryDataListener = new RepositoryDataListener(restoreRequest, repository);
            repository.getRepositoryData(repositoryDataListener);
            repositoryDataListener.waitForCompletion();
            snapshotInfo = repositoryDataListener.getSnapshotInfo();

        } finally {
            setCurrentThreadName(threadName);
        }
        return snapshotInfo;
    }

    private static void setCurrentThreadName(final String name) {
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                Thread.currentThread().setName(name);
                return null;
            }
        });
    }

    private static final class RepositoryDataListener implements ActionListener<RepositoryData> {

        private final RestoreSnapshotRequest restoreRequest;
        private final Repository repository;
        private SnapshotInfo snapshotInfo = null;
        private Exception repositoryException = null;
        private final CountDownLatch latch = new CountDownLatch(1);

        public RepositoryDataListener(RestoreSnapshotRequest restoreRequest, Repository repository) {
            super();
            this.restoreRequest = restoreRequest;
            this.repository = repository;
        }

        @Override
        public void onResponse(RepositoryData repositoryData) {
            if (log.isTraceEnabled()) {
                log.trace("RepositoryDataListener got: " + repositoryData);
            }

            SnapshotId snapshotId = findSnapshot(restoreRequest.snapshot(), repositoryData);

            if (snapshotId != null) {

                if (log.isDebugEnabled()) {
                    log.debug("snapshot found: {} (UUID: {})", snapshotId.getName(), snapshotId.getUUID());
                }

                repository.getSnapshotInfo(snapshotId, new ActionListener<SnapshotInfo>() {

                    @Override
                    public void onResponse(SnapshotInfo response) {
                        snapshotInfo = response;
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Error in getSnapshotInfo()", e);
                        repositoryException = e;
                        latch.countDown();
                    }

                });

            } else {
                log.warn("Could not find snapshot " + restoreRequest.snapshot());
                latch.countDown();
            }

        }

        @Override
        public void onFailure(Exception e) {
            log.error("Error in RepositoryDataListener", e);
            repositoryException = e;
            latch.countDown();
        }

        public SnapshotInfo getSnapshotInfo() {
            return snapshotInfo;
        }

        public Exception getRepositoryException() {
            return repositoryException;
        }

        public void waitForCompletion() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error(e);
            }
        }

        private SnapshotId findSnapshot(String snapshotName, RepositoryData repositoryData) {

            for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                if (snapshotId.getName().equals(snapshotName)) {
                    return snapshotId;
                }
            }

            return null;
        }
    }

}
