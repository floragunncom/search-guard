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
package com.floragunn.searchsupport.action;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.action.Action.Request;
import com.floragunn.searchsupport.action.Action.UnparsedMessage;
import com.google.common.collect.ImmutableMap;

public class StandardRequests {
    public static class EmptyRequest extends Request {
        public EmptyRequest() {

        }

        public EmptyRequest(UnparsedMessage message) {
        }

        @Override
        public Object toBasicObject() {
            return null;
        }

    }

    public static class IdRequest extends Action.Request {
        private final String id;

        public IdRequest() {
            super();
            this.id = null;
        }

        public IdRequest(String id) {
            super();
            this.id = id;
        }

        public IdRequest(UnparsedMessage message) throws ConfigValidationException {
            super(message);
            this.id = message.requiredDocNode().getAsString("id");
        }

        @Override
        public Object toBasicObject() {
            return ImmutableMap.of("id", id);
        }

        public String getId() {
            return id;
        }
    }
}
