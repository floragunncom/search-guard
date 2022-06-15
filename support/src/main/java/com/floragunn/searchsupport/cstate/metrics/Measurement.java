/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchsupport.cstate.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.UnparsedDocument;

public abstract class Measurement<M> implements Document<Measurement<M>> {
    private static final Logger log = LogManager.getLogger(Measurement.class);

    public static Measurement<?> parse(UnparsedDocument<Measurement<?>> unparsedDocument) {
        return parse(unparsedDocument.toDocNode());
    }

    public static Measurement<?> parse(DocNode docNode) {
        if (!docNode.isMap() || docNode.isEmpty()) {
            log.error("Error while parsing measurement\n" + docNode, docNode);
            return new UnknownMeasurement("_", docNode);
        }

        String type = docNode.keySet().iterator().next();
        DocNode typeNode = docNode.getAsNode(type);
        try {
            switch (type) {
            case "agg":
                if (typeNode.hasNonNull("agg_ms")) {
                    return new TimeAggregation.Milliseconds(typeNode);
                } else if (typeNode.hasNonNull("agg_ns")) {
                    return new TimeAggregation.Nanoseconds(typeNode);
                } else {
                    return new CountAggregation(typeNode);
                }
            case CacheStats.TYPE:
                return new CacheStats.Static(typeNode);
            case Count.TYPE:
                return new Count(typeNode);
            default:
                return new UnknownMeasurement(type, typeNode);
            }
        } catch (Exception e) {
            log.error("Error while parsing measurement " + type + "\n" + docNode, docNode);
            return new UnknownMeasurement(type, typeNode);
        }
    }

    public abstract Measurement<M> clone();

    public abstract void addToThis(M other);

    public abstract void addToThis(Measurement<?> other);

    public abstract String getType();

    public abstract void reset();

}
