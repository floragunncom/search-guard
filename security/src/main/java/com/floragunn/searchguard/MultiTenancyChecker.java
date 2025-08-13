/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.searchguard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersions;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.quote;

class MultiTenancyChecker {

    /**
     * Control if bootstrap check related to multi-tenancy is performed during system start time. The bootstrap check should
     *  be used with the SearchGuard version which does not contain production-ready support for the multi-tenancy to warn
     *  system administrator that the version of SearchGuard is inappropriate for the environment where multi-tenancy was used with the
     *  previous version of SearchGuard (which supported multi-tenancy).
     */
    public static final String SEARCHGUARD_MT_BOOTSTRAP_CHECK_ENABLED = "searchguard.multi_tenancy_bootstrap_check_enabled";

    private static final Logger log = LogManager.getLogger(MultiTenancyChecker.class);
    public static final Pattern FRONTEND_MT_INDEX_PATTERN = Pattern.compile(quote(".kibana") + "_-?\\d+_[a-z0-9]+_?[0-9.]+_\\d{3}");

    private final IndexRepository indexRepository;
    private final Settings settings;

    public MultiTenancyChecker(Settings settings, IndexRepository indexRepository) {
        this.settings = Objects.requireNonNull(settings, "Settings is required");
        this.indexRepository = Objects.requireNonNull(indexRepository, "Client is required");
    }

    public Optional<String> findMultiTenancyConfigurationError() {
        if(settings.getAsBoolean(SEARCHGUARD_MT_BOOTSTRAP_CHECK_ENABLED, false)) {
            List<String> indices = indexRepository.findIndicesMetadata() //
                .entrySet() //
                .stream() //
                .filter(entry -> entry.getValue().getCreationVersion().before(IndexVersions.V_8_8_0)) //
                .map(Map.Entry::getKey) //
                .filter(currentIndex -> FRONTEND_MT_INDEX_PATTERN.matcher(currentIndex).matches()) //
                .toList();
            if (!indices.isEmpty()) {
                String names = indices.stream().map(index -> "'" + index + "'").collect(Collectors.joining(", "));
                log.debug("Indices related to MT exist {}", names);
                return Optional.of("This version of SearchGuard does not include multi-tenancy, " +
                    "but indices related to the multi-tenancy created by ES prior to 8.8.0 exist: " + names +
                    ". Please get in touch with the support team.");
            } else {
                log.debug("Indices related to MT create by ES before version 8.8.0 were not found.");
                return Optional.empty();
            }
        } else {
            log.debug("MT bootstrap check is disabled.");
            return Optional.empty();
        }
    }

    public static class IndexRepository {

        private final BootstrapContext context;


        public IndexRepository(BootstrapContext context) {
            this.context = Objects.requireNonNull(context, "Bootstrap is required");
        }

        public Map<String, IndexMetadata> findIndicesMetadata() {
            Map<String, IndexMetadata> indicesMetadata = context.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).indices();
            if(log.isInfoEnabled()) {
                String names = indicesMetadata.entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + "@" + entry.getValue().getCreationVersion())
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));
                log.debug("Existing indices with ES version which created the index {}.", names);
            }
            return indicesMetadata;
        }
    }
}
