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

package com.floragunn.searchguard.authc.blocking;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableList;


/**
 * Deprecation pre-notice: this configuration tries to achieve too many things at once. Also, for dynamic updates (which you might want
 * to do for blocks), the sg config approach might be not the best. This should be simplified and moved to a dedicated index.
 */
public class Blocks implements Document<Blocks> {

    public static ValidationResult<Blocks> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        String description = vNode.get("description").asString();
        Type type = vNode.get("type").asEnum(Type.class);
        Verdict verdict = vNode.get("verdict").asEnum(Verdict.class);
        ImmutableList<String> value = ImmutableList.of(vNode.get("value").required().asListOfStrings());

        vNode.checkForUnusedAttributes();

        return new ValidationResult<Blocks>(new Blocks(docNode, type, verdict, value, description),
                validationErrors);
    }

    
    private final DocNode source;
    private final Type type;
    private final Verdict verdict;
    private final ImmutableList<String> value;
    private final String description;
    
    public Blocks(DocNode source, Type type, Verdict verdict, ImmutableList<String> value, String description) {
        this.source = source;
        this.type = type;
        this.verdict = verdict;
        this.value = value;
        this.description = description;
    }
    
    public Type getType() {
        return type;
    }

    public Verdict getVerdict() {
        return verdict;
    }

    public ImmutableList<String> getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }
    
    @Override
    public Object toBasicObject() {
        return source;
    }
    
    public enum Type {
        ip("ip"), name("name"), net_mask("net_mask");

        private final String type;

        Type(String name) {
            type = name;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return type;
        }
    }

    public enum Verdict {
        allow("allow"), disallow("disallow");

        private final String verdict;

        Verdict(String name) {
            verdict = name;
        }

        public String getVerdict() {
            return verdict;
        }

        @Override
        public String toString() {
            return verdict;
        }
    }

}
