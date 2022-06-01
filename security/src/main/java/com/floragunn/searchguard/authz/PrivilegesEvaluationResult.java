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

package com.floragunn.searchguard.authz;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;

public class PrivilegesEvaluationResult {

    public static final PrivilegesEvaluationResult OK = new PrivilegesEvaluationResult(Status.OK);
    public static final PrivilegesEvaluationResult PARTIALLY_OK = new PrivilegesEvaluationResult(Status.PARTIALLY_OK);
    public static final PrivilegesEvaluationResult EMPTY = new PrivilegesEvaluationResult(Status.EMPTY);

    public static final PrivilegesEvaluationResult INSUFFICIENT = new PrivilegesEvaluationResult(Status.INSUFFICIENT);
    public static final PrivilegesEvaluationResult PENDING = new PrivilegesEvaluationResult(Status.PENDING);

    private final Status status;
    private final CheckTable<String, Action> indexToActionPrivilegeTable;
    private final ImmutableList<Error> errors;
    private final ImmutableSet<String> availableIndices;
    private final String reason;
    private final ImmutableList<ActionFilter> additionalActionFilters;

    PrivilegesEvaluationResult(Status status) {
        this.status = status;
        this.indexToActionPrivilegeTable = null;
        this.errors = ImmutableList.empty();
        this.reason = null;
        this.availableIndices = null;
        this.additionalActionFilters = ImmutableList.empty();
    }

    PrivilegesEvaluationResult(Status status, String reason, ImmutableSet<String> availableIndices,
            CheckTable<String, Action> indexToActionPrivilegeTable, ImmutableList<Error> errors,
            ImmutableList<ActionFilter> additionalActionFilters) {
        this.status = status;
        this.indexToActionPrivilegeTable = indexToActionPrivilegeTable;
        this.errors = errors;
        this.reason = reason;
        this.availableIndices = availableIndices;
        this.additionalActionFilters = additionalActionFilters;
    }

    public PrivilegesEvaluationResult reason(String reason) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.indexToActionPrivilegeTable, this.errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult reason(String reason, ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.indexToActionPrivilegeTable, errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult reason(String reason, Error error) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.indexToActionPrivilegeTable, ImmutableList.of(errors),
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(CheckTable<String, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, indexToActionPrivilegeTable, this.errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(CheckTable<String, Action> indexToActionPrivilegeTable, ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, indexToActionPrivilegeTable, errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(String reason, CheckTable<String, Action> indexToActionPrivilegeTable, ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, indexToActionPrivilegeTable, errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(ImmutableList<Error> errors) {
        if (errors.size() != 0) {
            return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, indexToActionPrivilegeTable, errors,
                    this.additionalActionFilters);
        } else {
            return this;
        }
    }

    public PrivilegesEvaluationResult with(ActionFilter additionalActionFilter) {
        if (additionalActionFilter != null) {
            return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, this.indexToActionPrivilegeTable, this.errors,
                    this.additionalActionFilters.with(additionalActionFilter));
        } else {
            return this;
        }
    }

    public PrivilegesEvaluationResult availableIndices(ImmutableSet<String> availableIndices, CheckTable<String, Action> indexToActionPrivilegeTable,
            ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices, indexToActionPrivilegeTable, errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult availableIndices(ImmutableSet<String> availableIndices,
            CheckTable<String, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices, indexToActionPrivilegeTable, errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult missingPrivileges(Action action) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, CheckTable.create("_", ImmutableSet.of(action)),
                this.errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult status(Status status) {
        return new PrivilegesEvaluationResult(status, this.reason, this.availableIndices, this.indexToActionPrivilegeTable, this.errors,
                this.additionalActionFilters);
    }

    public CheckTable<String, Action> getIndexToActionPrivilegeTable() {
        return indexToActionPrivilegeTable;
    }

    public ImmutableList<Error> getErrors() {
        return errors;
    }

    public Throwable getFirstThrowable() {
        if (errors.isEmpty()) {
            return null;
        }

        for (Error error : errors) {
            if (error.cause != null) {
                return error.cause;
            }
        }

        return null;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public Status getStatus() {
        return status;
    }

    public boolean isOk() {
        return status == Status.OK;
    }

    public boolean isPending() {
        return status == Status.PENDING;
    }
    
    public ImmutableSet<String> getAvailableIndices() {
        return availableIndices;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("");

        if (reason != null) {
            result.append("Reason: ").append(reason).append("\n");
        }

        if (indexToActionPrivilegeTable != null) {
            String evaluatedPrivileges = indexToActionPrivilegeTable.toString("ok", "MISSING");

            if (evaluatedPrivileges.length() > 30 || evaluatedPrivileges.contains("\n")) {
                result.append("Evaluated Privileges:\n").append(evaluatedPrivileges).append("\n");
            } else {
                result.append("Evaluated Privileges: ").append(evaluatedPrivileges).append("\n");
            }
        }

        if (errors.size() == 1) {
            result.append("Errors: ").append(errors.only());
        } else if (errors.size() > 1) {
            result.append("Errors:\n").append(errors.stream().map((e) -> " - " + e + "\n").collect(Collectors.toList())).append("\n");
        }

        return result.toString();
    }

    public static enum Status {
        OK, PARTIALLY_OK, EMPTY, INSUFFICIENT, PENDING;
    }

    public static class Error {

        private final String message;
        private final Throwable cause;
        private final String role;
        private final Throwable rootCause;

        public Error(String message, Throwable cause) {
            this.message = message;
            this.cause = cause;
            this.role = null;
            this.rootCause = getRootCause(cause);
        }

        public Error(String message, Throwable cause, String role) {
            this.message = message;
            this.cause = cause;
            this.role = role;
            this.rootCause = getRootCause(cause);
        }

        public String getMessage() {
            return message;
        }

        public Throwable getCause() {
            return cause;
        }

        @Override
        public String toString() {
            if (rootCause != null) {
                return message + " [" + rootCause + "]";
            } else {
                return message;
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((message == null) ? 0 : message.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Error)) {
                return false;
            }
            Error other = (Error) obj;
            if (message == null) {
                if (other.message != null) {
                    return false;
                }
            } else if (!message.equals(other.message)) {
                return false;
            }
            return true;
        }

        public String getRole() {
            return role;
        }

        private static Throwable getRootCause(Throwable t) {
            if (t == null) {
                return null;
            }

            for (int i = 0; t.getCause() != null && t.getCause() != t && i < 10; i++) {
                t = t.getCause();
            }

            return t;
        }
    }

    public ImmutableList<ActionFilter> getAdditionalActionFilters() {
        return additionalActionFilters;
    }

    public boolean hasAdditionalActionFilters() {
        return additionalActionFilters != null && additionalActionFilters.size() > 0;
    }

    public Exception toSecurityException(PrivilegesEvaluationContext context) {
        ElasticsearchSecurityException result = new ElasticsearchSecurityException("Insufficient permissions", RestStatus.FORBIDDEN);

        if (this.indexToActionPrivilegeTable != null) {
            if (!isRelatedToIndexPermission()) {
                result.addMetadata("es.missing_permissions",
                        this.indexToActionPrivilegeTable.getColumns().stream().map((a) -> a.name()).collect(Collectors.toList()));

            } else {
                result.addMetadata("es.missing_permissions", getFlattenedIndexToActionPrivilegeTable());
            }
        }

        if (context.isDebugEnabled()) {
            if (reason != null) {
                result.addMetadata("es.reason_detail", reason);
            }

            result.addMetadata("es.user", String.valueOf(context.getUser()));

            if (context.getMappedRoles() != null) {
                result.addMetadata("es.effective_roles", context.getMappedRoles().stream().collect(Collectors.toList()));
            }

            result.addMetadata("es.user_attributes", context.getUser().getStructuredAttributes().keySet().stream().collect(Collectors.toList()));
            
            if (errors != null && !errors.isEmpty()) {
                result.addMetadata("es.errors", errors.stream().map((e) -> e.toString()).collect(Collectors.toList()));
            }
        }

        return result;

    }

    private boolean isRelatedToIndexPermission() {
        return this.indexToActionPrivilegeTable != null && this.indexToActionPrivilegeTable.getColumns().any().isIndexPrivilege();
    }

    private List<String> getFlattenedIndexToActionPrivilegeTable() {
        List<String> result = new ArrayList<>();

        for (String index : this.indexToActionPrivilegeTable.getRows()) {
            for (Action action : this.indexToActionPrivilegeTable.getColumns()) {
                if (!this.indexToActionPrivilegeTable.isChecked(index, action)) {
                    result.add(index + ": " + action);
                }
            }
        }

        return result;
    }
}
