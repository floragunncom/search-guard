/*
 * Copyright 2025 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.rest.RestRequest;

import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Reader which supports both full and stream request bodies.
 */
class AuditRestRequestBodyReader {

    private final static Logger log = LogManager.getLogger(AuditRestRequestBodyReader.class);

    /**
     * Reads the request body and passes it to the consumer
     */
    static void readRequestBody(RestRequest request, Consumer<BytesReference> bodyConsumer) {
        if (request.isFullContent()) {
            bodyConsumer.accept(request.requiredContent());
        } else if (request.isStreamedContent()) {
            StreamBodyHandler streamBodyHandler = new StreamBodyHandler(bodyConsumer);
            request.contentStream().addTracingHandler(streamBodyHandler);
        } else {
            log.error("Unknown request content type");
            assert false : "Unknown request content type, it's neither full nor stream.";
        }
    }

    static class StreamBodyHandler implements HttpBody.ChunkHandler {

        private final Consumer<BytesReference> aggregatedBodyConsumer;
        private ArrayList<ReleasableBytesReference> chunks;

        private StreamBodyHandler(Consumer<BytesReference> aggregatedBodyConsumer) {
            this.aggregatedBodyConsumer = aggregatedBodyConsumer;
        }

        @Override
        public void onNext(ReleasableBytesReference chunk, boolean isLast) {
            if (isLast) {
                if (chunks == null) {
                    aggregatedBodyConsumer.accept(chunk);
                } else {
                    chunks.add(chunk);
                    BytesReference compositeBytes = CompositeBytesReference.of(chunks.toArray(new ReleasableBytesReference[0]));
                    aggregatedBodyConsumer.accept(compositeBytes);
                }
            } else {
                if (chunks == null) {
                    chunks = new ArrayList<>();
                }
                chunks.add(chunk);
            }
        }
    }

}
