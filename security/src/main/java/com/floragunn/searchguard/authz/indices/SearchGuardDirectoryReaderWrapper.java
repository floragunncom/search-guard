/*
 * Copyright 2015-2022 floragunn GmbH
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
package com.floragunn.searchguard.authz.indices;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.User;
import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;

public class SearchGuardDirectoryReaderWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {

    private final ThreadContext threadContext;
    private final Index index;
    private final AdminDNs adminDns;
    private final ImmutableList<CheckedFunction<DirectoryReader, DirectoryReader, IOException>> moreWrappersForNormalOperations;
    private final ImmutableList<CheckedFunction<DirectoryReader, DirectoryReader, IOException>> moreWrappersForAllOperations;

    //constructor is called per index, so avoid costly operations here
    public SearchGuardDirectoryReaderWrapper(IndexService indexService, AdminDNs adminDNs,
            ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> moreWrappersForNormalOperations,
            ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> moreWrappersForAllOperations) {
        this.index = indexService.index();
        this.threadContext = indexService.getThreadPool().getThreadContext();
        this.adminDns = adminDNs;

        this.moreWrappersForNormalOperations = createWrappers(indexService, moreWrappersForNormalOperations);
        this.moreWrappersForAllOperations = createWrappers(indexService, moreWrappersForAllOperations);
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {
        if (isAdminAuthenticatedOrInternalRequest()) {
            for (CheckedFunction<DirectoryReader, DirectoryReader, IOException> nextWrapper : moreWrappersForAllOperations) {
                reader = nextWrapper.apply(reader);
            }

            return reader;
        }

        if (isSearchGuardIndexRequest()) {
            return new EmptyFilterLeafReader.EmptyDirectoryReader(reader);
        }

        for (CheckedFunction<DirectoryReader, DirectoryReader, IOException> nextWrapper : moreWrappersForNormalOperations) {
            reader = nextWrapper.apply(reader);
        }

        for (CheckedFunction<DirectoryReader, DirectoryReader, IOException> nextWrapper : moreWrappersForAllOperations) {
            reader = nextWrapper.apply(reader);
        }

        return reader;
    }

    protected final boolean isAdminAuthenticatedOrInternalRequest() {
        final User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);

        if (user != null && adminDns.isAdmin(user)) {
            return true;
        }

        if ("true".equals(HeaderHelper.getSafeFromHeader(threadContext, ConfigConstants.SG_CONF_REQUEST_HEADER))) {
            return true;
        }

        return false;
    }

    protected final boolean isSearchGuardIndexRequest() {
        return SearchGuardPlugin.getProtectedIndices().isProtected(index.getName());
    }

    private static ImmutableList<CheckedFunction<DirectoryReader, DirectoryReader, IOException>> createWrappers(IndexService indexService,
            ImmutableList<Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>>> moreWrappers) {
        return moreWrappers.map((v) -> v.apply(indexService));
    }

    /* The class EmptyFilterLeafReader is based on https://github.com/apache/lucene-solr/blob/1d85cd783863f75cea133fb9c452302214165a4d/lucene/test-framework/src/java/org/apache/lucene/index/AllDeletedFilterReader.java
     * from Apache 2 licensed Apache Solr project.
     *
     * Original license header:
     *
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements.  See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License.  You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    static class EmptyFilterLeafReader extends FilterLeafReader {

        final Bits liveDocs;

        public EmptyFilterLeafReader(LeafReader in) {
            super(in);
            liveDocs = new Bits.MatchNoBits(in.maxDoc());
            assert maxDoc() == 0 || hasDeletions();
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return 0;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }

        private static class EmptySubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

            @Override
            public LeafReader wrap(final LeafReader reader) {
                return new EmptyFilterLeafReader(reader);
            }

        }

        static class EmptyDirectoryReader extends FilterDirectoryReader {

            public EmptyDirectoryReader(final DirectoryReader in) throws IOException {
                super(in, new EmptySubReaderWrapper());
            }

            @Override
            protected DirectoryReader doWrapDirectoryReader(final DirectoryReader in) throws IOException {
                return new EmptyDirectoryReader(in);
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        }
    }
}
