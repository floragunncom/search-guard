/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.queries;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.FieldMaskingSpanQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SpanContainingQueryBuilder;
import org.elasticsearch.index.query.SpanFirstQueryBuilder;
import org.elasticsearch.index.query.SpanMultiTermQueryBuilder;
import org.elasticsearch.index.query.SpanNearQueryBuilder;
import org.elasticsearch.index.query.SpanNotQueryBuilder;
import org.elasticsearch.index.query.SpanOrQueryBuilder;
import org.elasticsearch.index.query.SpanWithinQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

public abstract class QueryBuilderTraverser {

    public static boolean exists(QueryBuilder queryBuilder, Predicate<QueryBuilder> predicate) {
        Exists traverser = new Exists(predicate);

        return traverser.check(queryBuilder);
    }

    public static QueryBuilder find(QueryBuilder queryBuilder, Predicate<QueryBuilder> predicate) {
        Exists traverser = new Exists(predicate);

        if (traverser.check(queryBuilder)) {
            return traverser.matched;
        } else {
            return null;
        }
    }

    public static Set<QueryBuilder> findAll(QueryBuilder queryBuilder, Predicate<QueryBuilder> predicate) {
        Complete traverser = new Complete(predicate);

        traverser.check(queryBuilder);

        return traverser.matched;
    }

    public boolean check(String query, NamedXContentRegistry namedXContentRegistry) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY.withRegistry(namedXContentRegistry),
                query);
        QueryBuilder queryBuilder = AbstractQueryBuilder.parseTopLevelQuery(parser);

        return check(queryBuilder);
    }

    public abstract boolean check(QueryBuilder queryBuilder);

    public abstract boolean check(List<? extends QueryBuilder> queryBuilders);

    public static class Exists extends QueryBuilderTraverser {
        private final Predicate<QueryBuilder> predicate;
        private QueryBuilder matched;

        public Exists(Predicate<QueryBuilder> predicate) {
            this.predicate = predicate;
        }

        public boolean check(QueryBuilder queryBuilder) {
            if (queryBuilder == null) {
                return false;
            }

            if (predicate.test(queryBuilder)) {
                this.matched = queryBuilder;
                return true;
            }

            if (queryBuilder instanceof BoolQueryBuilder) {
                BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;

                return check(boolQueryBuilder.must()) || check(boolQueryBuilder.mustNot()) || check(boolQueryBuilder.should())
                        || check(boolQueryBuilder.filter());
            } else if (queryBuilder instanceof BoostingQueryBuilder) {
                BoostingQueryBuilder boostingQueryBuilder = (BoostingQueryBuilder) queryBuilder;

                return check(boostingQueryBuilder.positiveQuery()) || check(boostingQueryBuilder.negativeQuery());
            } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
                ConstantScoreQueryBuilder constantScoreQueryBuilder = (ConstantScoreQueryBuilder) queryBuilder;

                return check(constantScoreQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof DisMaxQueryBuilder) {
                DisMaxQueryBuilder disMaxQueryBuilder = (DisMaxQueryBuilder) queryBuilder;

                return check(disMaxQueryBuilder.innerQueries());
            } else if (queryBuilder instanceof FieldMaskingSpanQueryBuilder) {
                FieldMaskingSpanQueryBuilder fieldMaskingSpanQueryBuilder = (FieldMaskingSpanQueryBuilder) queryBuilder;

                return check(fieldMaskingSpanQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
                FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;

                return check(functionScoreQueryBuilder.query());
            } else if (queryBuilder instanceof NestedQueryBuilder) {
                NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) queryBuilder;

                return check(nestedQueryBuilder.query());
            } else if (queryBuilder instanceof SpanContainingQueryBuilder) {
                SpanContainingQueryBuilder spanContainingQueryBuilder = (SpanContainingQueryBuilder) queryBuilder;

                return check(spanContainingQueryBuilder.bigQuery()) || check(spanContainingQueryBuilder.littleQuery());
            } else if (queryBuilder instanceof SpanFirstQueryBuilder) {
                SpanFirstQueryBuilder spanFirstQueryBuilder = (SpanFirstQueryBuilder) queryBuilder;

                return check(spanFirstQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof SpanMultiTermQueryBuilder) {
                SpanMultiTermQueryBuilder spanMultiTermQueryBuilder = (SpanMultiTermQueryBuilder) queryBuilder;

                return check(spanMultiTermQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof SpanNearQueryBuilder) {
                SpanNearQueryBuilder spanNearQueryBuilder = (SpanNearQueryBuilder) queryBuilder;

                return check(spanNearQueryBuilder.clauses());
            } else if (queryBuilder instanceof SpanNotQueryBuilder) {
                SpanNotQueryBuilder spanNotQueryBuilder = (SpanNotQueryBuilder) queryBuilder;

                return check(spanNotQueryBuilder.excludeQuery()) || check(spanNotQueryBuilder.includeQuery());
            } else if (queryBuilder instanceof SpanOrQueryBuilder) {
                SpanOrQueryBuilder spanOrQueryBuilder = (SpanOrQueryBuilder) queryBuilder;

                return check(spanOrQueryBuilder.clauses());
            } else if (queryBuilder instanceof SpanWithinQueryBuilder) {
                SpanWithinQueryBuilder spanWithinQueryBuilder = (SpanWithinQueryBuilder) queryBuilder;

                return check(spanWithinQueryBuilder.bigQuery()) || check(spanWithinQueryBuilder.littleQuery());
            } else {
                return false;
            }
        }

        public boolean check(List<? extends QueryBuilder> queryBuilders) {
            for (QueryBuilder queryBuilder : queryBuilders) {
                if (check(queryBuilder)) {
                    return true;
                }
            }

            return false;
        }

        public QueryBuilder getMatched() {
            return matched;
        }
    }

    public static class Complete extends QueryBuilderTraverser {
        private final Predicate<QueryBuilder> predicate;
        private Set<QueryBuilder> matched = new HashSet<>();

        public Complete(Predicate<QueryBuilder> predicate) {
            this.predicate = predicate;
        }

        public boolean check(QueryBuilder queryBuilder) {
            if (queryBuilder == null) {
                return true;
            }

            boolean matched = true;

            if (queryBuilder instanceof BoolQueryBuilder) {
                BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;

                matched = check(boolQueryBuilder.must()) & check(boolQueryBuilder.mustNot()) & check(boolQueryBuilder.should())
                        & check(boolQueryBuilder.filter());
            } else if (queryBuilder instanceof BoostingQueryBuilder) {
                BoostingQueryBuilder boostingQueryBuilder = (BoostingQueryBuilder) queryBuilder;

                matched = check(boostingQueryBuilder.positiveQuery()) & check(boostingQueryBuilder.negativeQuery());
            } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
                ConstantScoreQueryBuilder constantScoreQueryBuilder = (ConstantScoreQueryBuilder) queryBuilder;

                matched = check(constantScoreQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof DisMaxQueryBuilder) {
                DisMaxQueryBuilder disMaxQueryBuilder = (DisMaxQueryBuilder) queryBuilder;

                matched = check(disMaxQueryBuilder.innerQueries());
            } else if (queryBuilder instanceof FieldMaskingSpanQueryBuilder) {
                FieldMaskingSpanQueryBuilder fieldMaskingSpanQueryBuilder = (FieldMaskingSpanQueryBuilder) queryBuilder;

                matched = check(fieldMaskingSpanQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
                FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;

                matched = check(functionScoreQueryBuilder.query());
            } else if (queryBuilder instanceof NestedQueryBuilder) {
                NestedQueryBuilder nestedQueryBuilder = (NestedQueryBuilder) queryBuilder;

                matched = check(nestedQueryBuilder.query());
            } else if (queryBuilder instanceof SpanContainingQueryBuilder) {
                SpanContainingQueryBuilder spanContainingQueryBuilder = (SpanContainingQueryBuilder) queryBuilder;

                matched = check(spanContainingQueryBuilder.bigQuery()) & check(spanContainingQueryBuilder.littleQuery());
            } else if (queryBuilder instanceof SpanFirstQueryBuilder) {
                SpanFirstQueryBuilder spanFirstQueryBuilder = (SpanFirstQueryBuilder) queryBuilder;

                matched = check(spanFirstQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof SpanMultiTermQueryBuilder) {
                SpanMultiTermQueryBuilder spanMultiTermQueryBuilder = (SpanMultiTermQueryBuilder) queryBuilder;

                matched = check(spanMultiTermQueryBuilder.innerQuery());
            } else if (queryBuilder instanceof SpanNearQueryBuilder) {
                SpanNearQueryBuilder spanNearQueryBuilder = (SpanNearQueryBuilder) queryBuilder;

                matched = check(spanNearQueryBuilder.clauses());
            } else if (queryBuilder instanceof SpanNotQueryBuilder) {
                SpanNotQueryBuilder spanNotQueryBuilder = (SpanNotQueryBuilder) queryBuilder;

                matched = check(spanNotQueryBuilder.excludeQuery()) & check(spanNotQueryBuilder.includeQuery());
            } else if (queryBuilder instanceof SpanOrQueryBuilder) {
                SpanOrQueryBuilder spanOrQueryBuilder = (SpanOrQueryBuilder) queryBuilder;

                matched = check(spanOrQueryBuilder.clauses());
            } else if (queryBuilder instanceof SpanWithinQueryBuilder) {
                SpanWithinQueryBuilder spanWithinQueryBuilder = (SpanWithinQueryBuilder) queryBuilder;

                matched = check(spanWithinQueryBuilder.bigQuery()) & check(spanWithinQueryBuilder.littleQuery());
            }

            if (predicate.test(queryBuilder)) {
                this.matched.add(queryBuilder);
                return matched;
            } else {
                return false;
            }
        }

        public boolean check(List<? extends QueryBuilder> queryBuilders) {
            boolean matched = true;

            for (QueryBuilder queryBuilder : queryBuilders) {
                matched &= check(queryBuilder);
            }

            return matched;
        }

        public Set<QueryBuilder> getMatched() {
            return matched;
        }
    }
}
