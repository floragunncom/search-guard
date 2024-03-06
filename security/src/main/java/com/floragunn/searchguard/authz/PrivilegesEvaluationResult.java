/*
 * Copyright 2021-2024 floragunn GmbH
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
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.rest.RestStatus;

import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector;
import com.floragunn.searchsupport.meta.Meta;

public class PrivilegesEvaluationResult {

    /**
     * The user has all necessary privileges for a action request
     */
    public static final PrivilegesEvaluationResult OK = new PrivilegesEvaluationResult(Status.OK);

    /**
     * The user does not have privileges for all requested indices (or aliases or data streams), but for some of the requested indices (or aliases or data streams)
     */
    public static final PrivilegesEvaluationResult PARTIALLY_OK = new PrivilegesEvaluationResult(Status.PARTIALLY_OK);

    /**
     * The user does not have privileges for the requested aliases, but for all indices requested by the aliases.
     */
    public static final PrivilegesEvaluationResult OK_WHEN_RESOLVED = new PrivilegesEvaluationResult(Status.OK_WHEN_RESOLVED);

    /**
     * The user does not have any privileges for the requested indices. The user shall get an empty result as response.
     */
    public static final PrivilegesEvaluationResult EMPTY = new PrivilegesEvaluationResult(Status.EMPTY);

    /**
     * The user does not have any privileges for the requested indices. The user shall get an error as response.
     */
    public static final PrivilegesEvaluationResult INSUFFICIENT = new PrivilegesEvaluationResult(Status.INSUFFICIENT);

    /**
     * The current code could not finally determine the authorization status.
     */
    public static final PrivilegesEvaluationResult PENDING = new PrivilegesEvaluationResult(Status.PENDING);

    private final Status status;
    private final CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable;
    private final ImmutableList<Error> errors;
    private final ImmutableSet<String> availableIndices;
    private final ImmutableMap<Action.AdditionalDimension, ImmutableSet<String>> additionalAvailableIndices;
    private final ImmutableMap<Action.AdditionalDimension, CheckTable<Meta.IndexLikeObject, Action>> additionalIndexToActionPrivilegeTables;

    private final String reason;
    private final ImmutableList<ActionFilter> additionalActionFilters;

    PrivilegesEvaluationResult(Status status) {
        this.status = status;
        this.indexToActionPrivilegeTable = null;
        this.errors = ImmutableList.empty();
        this.reason = null;
        this.availableIndices = null;
        this.additionalActionFilters = ImmutableList.empty();
        this.additionalAvailableIndices = ImmutableMap.empty();
        this.additionalIndexToActionPrivilegeTables = ImmutableMap.empty();
    }

    PrivilegesEvaluationResult(Status status, String reason, ImmutableSet<String> availableIndices,
            ImmutableMap<Action.AdditionalDimension, ImmutableSet<String>> additionalAvailableIndices,
            CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable,
            ImmutableMap<Action.AdditionalDimension, CheckTable<Meta.IndexLikeObject, Action>> additionalIndexToActionPrivilegeTables,
            ImmutableList<Error> errors, ImmutableList<ActionFilter> additionalActionFilters) {
        this.status = status;
        this.indexToActionPrivilegeTable = indexToActionPrivilegeTable;
        this.errors = errors;
        this.reason = reason;
        this.availableIndices = availableIndices;
        this.additionalAvailableIndices = additionalAvailableIndices != null ? additionalAvailableIndices : ImmutableMap.empty();
        this.additionalIndexToActionPrivilegeTables = additionalIndexToActionPrivilegeTables != null ? additionalIndexToActionPrivilegeTables
                : ImmutableMap.empty();
        this.additionalActionFilters = additionalActionFilters;
    }

    public PrivilegesEvaluationResult reason(String reason) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.additionalAvailableIndices,
                this.indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, this.errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult reason(String reason, ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.additionalAvailableIndices,
                this.indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult reason(String reason, Error error) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.additionalAvailableIndices,
                this.indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, ImmutableList.of(errors),
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, this.additionalAvailableIndices,
                indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, this.errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable, ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, this.additionalAvailableIndices,
                indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(String reason, CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable,
            ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.additionalAvailableIndices,
                indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult with(ImmutableList<Error> errors) {
        if (errors.size() != 0) {
            return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, this.additionalAvailableIndices,
                    indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors, this.additionalActionFilters);
        } else {
            return this;
        }
    }

    public PrivilegesEvaluationResult with(ActionFilter additionalActionFilter) {
        if (additionalActionFilter != null) {
            return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, this.additionalAvailableIndices,
                    this.indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, this.errors,
                    this.additionalActionFilters.with(additionalActionFilter));
        } else {
            return this;
        }
    }

    public PrivilegesEvaluationResult availableIndices(ImmutableSet<Meta.IndexLikeObject> availableIndices,
            CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable, ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices.map(Meta.IndexLikeObject::name),
                this.additionalAvailableIndices, indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult availableIndices(ImmutableSet<Meta.IndexLikeObject> availableIndices,
            CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices.map(Meta.IndexLikeObject::name),
                this.additionalAvailableIndices, indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors,
                this.additionalActionFilters);
    }

    /*
    public PrivilegesEvaluationResult availableIndices(ImmutableSet<String> availableIndices,
            CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices, this.additionalAvailableIndices,
                indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, errors, this.additionalActionFilters);
    }*/

    public PrivilegesEvaluationResult availableAdditionally(Action.AdditionalDimension role,
            ImmutableSet<String> addtionalAvailableIndices, CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable,
            ImmutableList<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices,
                this.additionalAvailableIndices.with(role, addtionalAvailableIndices), this.indexToActionPrivilegeTable,
                this.additionalIndexToActionPrivilegeTables.with(role, indexToActionPrivilegeTable), errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult availableAdditionally(Action.AdditionalDimension role,
            ImmutableSet<String> addtionalAvailableIndices, CheckTable<Meta.IndexLikeObject, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices,
                this.additionalAvailableIndices.with(role, addtionalAvailableIndices), this.indexToActionPrivilegeTable,
                this.additionalIndexToActionPrivilegeTables.with(role, indexToActionPrivilegeTable), errors, this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult withAdditional(Action.AdditionalDimension role,
            PrivilegesEvaluationResult additional) {
        Status status;

        if (this.status != additional.status) {
            if (additional.status == Status.INSUFFICIENT || this.status == Status.INSUFFICIENT) {
                status = Status.INSUFFICIENT;
            } else if (additional.status == Status.PENDING || this.status == Status.PENDING) {
                status = Status.PENDING;
            } else if (additional.status == Status.EMPTY || this.status == Status.EMPTY) {
                status = Status.EMPTY;
            } else if (additional.status == Status.OK_WHEN_RESOLVED) {
                if (this.status == Status.OK || this.status == Status.OK_WHEN_RESOLVED) {
                    status = Status.OK_WHEN_RESOLVED;
                } else {
                    status = this.status;
                }
            } else if (additional.status == Status.PARTIALLY_OK) {
                if (this.status == Status.OK || this.status == Status.OK_WHEN_RESOLVED || this.status == Status.PARTIALLY_OK) {
                    status = Status.PARTIALLY_OK;
                } else {
                    status = this.status;
                }
            } else {
                // result.status == Status.OK

                status = this.status;
            }
        } else {
            status = this.status;
        }

        String reason = this.reason;

        if (reason == null) {
            reason = additional.reason;
        } else if (additional.reason != null) {
            reason = this.reason + "\n" + additional.reason;
        }

        return new PrivilegesEvaluationResult(status, reason, availableIndices,
                additional.availableIndices != null ? this.additionalAvailableIndices.with(role, additional.availableIndices)
                        : this.additionalAvailableIndices,
                this.indexToActionPrivilegeTable,
                additional.indexToActionPrivilegeTable != null
                        ? this.additionalIndexToActionPrivilegeTables.with(role, additional.indexToActionPrivilegeTable)
                        : this.additionalIndexToActionPrivilegeTables,
                errors.with(additional.errors), this.additionalActionFilters.with(additional.additionalActionFilters));
    }

    public PrivilegesEvaluationResult missingPrivileges(Action action) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, this.additionalAvailableIndices,
                CheckTable.create(Meta.NonExistent.BLANK, ImmutableSet.of(action)), this.additionalIndexToActionPrivilegeTables, this.errors,
                this.additionalActionFilters);
    }

    public PrivilegesEvaluationResult status(Status status) {
        return new PrivilegesEvaluationResult(status, this.reason, this.availableIndices, this.additionalAvailableIndices,
                this.indexToActionPrivilegeTable, this.additionalIndexToActionPrivilegeTables, this.errors, this.additionalActionFilters);
    }

    public CheckTable<Meta.IndexLikeObject, Action> getIndexToActionPrivilegeTable() {
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
        try {
            StringBuilder result = new StringBuilder("");

            result.append("Status: ").append(status).append("\n");

            if (reason != null) {
                result.append("Reason: ").append(reason).append("\n");
            }

            if (indexToActionPrivilegeTable != null) {
                String evaluatedPrivileges = indexToActionPrivilegeTable.toString("ok", "MISSING");

                if (evaluatedPrivileges.length() > 30 || evaluatedPrivileges.contains("\n")) {
                    result.append("Evaluated privileges:\n").append(evaluatedPrivileges).append("\n");
                } else {
                    result.append("Evaluated privileges: ").append(evaluatedPrivileges).append("\n");
                }
            }

            if (!this.additionalIndexToActionPrivilegeTables.isEmpty()) {
                for (Map.Entry<Action.AdditionalDimension, CheckTable<Meta.IndexLikeObject, Action>> entry : this.additionalIndexToActionPrivilegeTables
                        .entrySet()) {
                    String evaluatedPrivileges = entry.getValue().toString("ok", "MISSING");

                    if (evaluatedPrivileges.length() > 30 || evaluatedPrivileges.contains("\n")) {
                        result.append("Evaluated privileges for ").append(entry.getKey()).append(":\n").append(evaluatedPrivileges).append("\n");
                    } else {
                        result.append("Evaluated privileges for ").append(entry.getKey()).append(": ").append(evaluatedPrivileges).append("\n");
                    }
                }
            }

            if (errors.size() == 1) {
                result.append("Errors: ").append(errors.only());
            } else if (errors.size() > 1) {
                result.append("Errors:\n").append(errors.stream().map((e) -> " - " + e + "\n").collect(Collectors.toList())).append("\n");
            }

            return result.toString();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static enum Status {
        /**
         * The user has all necessary privileges for a action request
         */
        OK,

        /**
        * The user does not have privileges for all requested indices (or aliases or data streams), but for some of the requested indices (or aliases or data streams)
        */
        PARTIALLY_OK,

        /**
         * The user does not have privileges for the requested aliases, but for all indices requested by the aliases.
         */
        OK_WHEN_RESOLVED,

        /**
         * The user does not have any privileges for the requested indices. The user shall get an empty result as response.
         */
        EMPTY,

        /**
         * The user does not have any privileges for the requested indices. The user shall get an error as response.
         */
        INSUFFICIENT,

        /**
         * The current code could not finally determine the authorization status.
         */
        PENDING;
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

        if (context.isDebugEnabled()) {
            if (this.indexToActionPrivilegeTable != null) {
                if (!isRelatedToIndexPermission()) {
                    result.addMetadata("es.missing_permissions",
                            this.indexToActionPrivilegeTable.getColumns().stream().map((a) -> a.name()).collect(Collectors.toList()));

                } else {
                    result.addMetadata("es.missing_permissions", getFlattenedIndexToActionPrivilegeTable());
                }
            }

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
        return this.indexToActionPrivilegeTable != null && !this.indexToActionPrivilegeTable.isEmpty()
                && this.indexToActionPrivilegeTable.getColumns().any().isIndexLikePrivilege();
    }

    private List<String> getFlattenedIndexToActionPrivilegeTable() {
        List<String> result = new ArrayList<>();

        for (Meta.IndexLikeObject index : this.indexToActionPrivilegeTable.getRows()) {
            for (Action action : this.indexToActionPrivilegeTable.getColumns()) {
                if (!this.indexToActionPrivilegeTable.isChecked(index, action)) {
                    result.add(index + ": " + action);
                }
            }
        }

        return result;
    }

    public ImmutableMap<Action.AdditionalDimension, ImmutableSet<String>> getAdditionalAvailableIndices() {
        return additionalAvailableIndices;
    }
}
