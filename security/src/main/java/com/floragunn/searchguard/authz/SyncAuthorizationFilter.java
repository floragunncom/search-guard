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

package com.floragunn.searchguard.authz;

import java.util.stream.Collectors;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.rest.RestStatus;

public interface SyncAuthorizationFilter {
    SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener);

    public class Result {
        public static final Result OK = new Result(Status.OK);
        public static final Result DENIED = new Result(Status.DENIED);
        public static final Result INTERCEPTED = new Result(Status.INTERCEPTED);

        private final Status status;
        private final String reason;
        private final Exception exception;

        Result(Status status) {
            this(status, null, null);
        }

        Result(Status status, String message, Exception exception) {
            this.status = status;
            this.reason = message;
            this.exception = exception;
        }

        public Result reason(String message) {
            return new Result(this.status, message, this.exception);
        }

        public Result cause(Exception exception) {
            return new Result(this.status, this.reason, exception);
        }
        
        public static enum Status {
            OK, DENIED, INTERCEPTED;
        }

        public Exception toSecurityException(PrivilegesEvaluationContext context) {
            OpenSearchSecurityException result = new OpenSearchSecurityException("Insufficient permissions", RestStatus.FORBIDDEN,
                    context.isDebugEnabled() ? exception : null);

            if (context.isDebugEnabled()) {
                if (reason != null) {
                    result.addMetadata("es.reason_detail", reason);
                }

                result.addMetadata("es.user", String.valueOf(context.getUser()));

                if (context.getMappedRoles() != null) {
                    result.addMetadata("es.effective_roles", context.getMappedRoles().stream().collect(Collectors.toList()));
                }

                result.addMetadata("es.user_attributes", context.getUser().getStructuredAttributes().keySet().stream().collect(Collectors.toList()));

            }

            return result;

        }

        public Status getStatus() {
            return status;
        }
    }
}
