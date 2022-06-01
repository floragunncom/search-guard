/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchsupport.queries;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchsupport.xcontent.XContentParserContext;

public class Query implements Document<Query> {
    private final QueryBuilder queryBuilder;
    private final String source;

    public Query(String source, XContentParserContext context) throws ConfigValidationException {
        try {
            this.source = source;
            XContentParser parser = JsonXContent.jsonXContent.createParser(context.xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    source);
            this.queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        } catch (Exception e) {
            throw new ConfigValidationException(new ValidationError(null, e.getMessage()).cause(e));
        }
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public String toString() {
        return source;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }
}
