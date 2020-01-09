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

package com.floragunn.searchguard.configuration;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.User;

public class SearchGuardIndexSearcherWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final ThreadContext threadContext;
    protected final Index index;
    private final AdminDNs adminDns;

    //constructor is called per index, so avoid costly operations here
	public SearchGuardIndexSearcherWrapper(final IndexService indexService, final Settings settings, final AdminDNs adminDNs) {
	    index = indexService.index();
	    threadContext = indexService.getThreadPool().getThreadContext();
        this.adminDns = adminDNs;
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {
        if (isSearchGuardIndexRequest() && !isAdminAuthenticatedOrInternalRequest()) {
            return new EmptyFilterLeafReader.EmptyDirectoryReader(reader);
        }

        return dlsFlsWrap(reader, isAdminAuthenticatedOrInternalRequest());
    }

    protected DirectoryReader dlsFlsWrap(final DirectoryReader reader, boolean isAdmin) throws IOException {
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

}
