/* 
 * Based on https://github.com/apache/lucene-solr/blob/branch_6_3/lucene/misc/src/java/org/apache/lucene/index/PKIndexSplitter.java
 * from Apache Lucene Solr, liensed under the Apache 2 license.
 * 
 * Original license:
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
 * 
 * 
 * Modifications:
 * 
 * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

class DlsGetEvaluator {
    private final Bits liveBits;
    private final int numDocs;
    private final CacheHelper readerCacheHelper;
    private final boolean hasDeletions;
    private static final Logger log = LogManager.getLogger(DlsGetEvaluator.class);

    public DlsGetEvaluator(FilterLeafReader filterLeafReader, Query dlsQuery, LeafReader in, boolean applyDlsHere) {
        try {
            log.trace("Creating DlsGetEvaluator\ndlsQuery: {}\napplyDlsHere: {}", dlsQuery, applyDlsHere);
            
            if (dlsQuery != null && applyDlsHere) {
                dlsQuery = dlsQuery.rewrite(filterLeafReader);
                final IndexSearcher searcher = new IndexSearcher(filterLeafReader);
                searcher.setQueryCache(null);
                final Weight preserveWeight = searcher.createWeight(dlsQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);

                final int maxDoc = in.maxDoc();
                final FixedBitSet bits = new FixedBitSet(maxDoc);
                final Scorer preserveScorer = preserveWeight.scorer(filterLeafReader.getContext());

                if (preserveScorer != null) {
                    bits.or(preserveScorer.iterator());
                }

                if (in.hasDeletions()) {
                    final Bits oldLiveDocs = in.getLiveDocs();
                    assert oldLiveDocs != null;
                    final DocIdSetIterator it = new BitSetIterator(bits, 0L);
                    for (int i = it.nextDoc(); i != DocIdSetIterator.NO_MORE_DOCS; i = it.nextDoc()) {
                        if (!oldLiveDocs.get(i)) {
                            bits.clear(i);
                        }
                    }
                }

                liveBits = bits;
                numDocs = in.numDocs();
                readerCacheHelper = null;
                hasDeletions = true;

            } else {
                //no dls or handled in a different place
                liveBits = in.getLiveDocs();
                numDocs = in.numDocs();
                readerCacheHelper = in.getReaderCacheHelper();
                hasDeletions = in.hasDeletions();
            }
        } catch (IOException e) {
            log.error("IOException in DlsGetEvaluator", e);
            throw new RuntimeException(e);
        }
    }

    //return null means no hidden docs
    public Bits getLiveDocs() {
        return liveBits;
    }

    public int numDocs() {
        return numDocs;
    }

    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }

    public boolean hasDeletions() {
        return hasDeletions;
    }
}