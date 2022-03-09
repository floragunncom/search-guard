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

package com.floragunn.searchguard.privileges;

import java.util.stream.Collectors;

import com.floragunn.fluent.collections.CheckTable;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.Action;

public class PrivilegesEvaluationResult {

    public static final PrivilegesEvaluationResult OK = new PrivilegesEvaluationResult(Status.OK);
    public static final PrivilegesEvaluationResult PARTIALLY_OK = new PrivilegesEvaluationResult(Status.PARTIALLY_OK);
    public static final PrivilegesEvaluationResult EMPTY = new PrivilegesEvaluationResult(Status.EMPTY);

    public static final PrivilegesEvaluationResult INSUFFICIENT = new PrivilegesEvaluationResult(Status.INSUFFICIENT);
    public static final PrivilegesEvaluationResult PENDING = new PrivilegesEvaluationResult(Status.PENDING);

    private final Status status;
    private final CheckTable<String, Action> indexToActionPrivilegeTable;
    private final ImmutableSet<Error> errors;
    private final ImmutableSet<String> availableIndices;
    private final String reason;

    PrivilegesEvaluationResult(Status status) {
        this.status = status;
        this.indexToActionPrivilegeTable = null;
        this.errors = ImmutableSet.empty();
        this.reason = null;
        this.availableIndices = null;
    }

    PrivilegesEvaluationResult(Status status, String reason, ImmutableSet<String> availableIndices, CheckTable<String, Action> indexToActionPrivilegeTable, ImmutableSet<Error> errors) {
        this.status = status;
        this.indexToActionPrivilegeTable = indexToActionPrivilegeTable;
        this.errors = errors;
        this.reason = reason;
        this.availableIndices = availableIndices;
    }

    public PrivilegesEvaluationResult reason(String reason) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.indexToActionPrivilegeTable, this.errors);
    }

    public PrivilegesEvaluationResult reason(String reason, ImmutableSet<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, this.indexToActionPrivilegeTable, errors);
    }

    public PrivilegesEvaluationResult reason(String reason, Error error) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices,  this.indexToActionPrivilegeTable, ImmutableSet.of(errors));
    }

    public PrivilegesEvaluationResult with(CheckTable<String, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices, indexToActionPrivilegeTable, this.errors);
    }

    public PrivilegesEvaluationResult with(CheckTable<String, Action> indexToActionPrivilegeTable, ImmutableSet<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, this.reason, this.availableIndices,  indexToActionPrivilegeTable, errors);
    }
    
    public PrivilegesEvaluationResult with(String reason, CheckTable<String, Action> indexToActionPrivilegeTable, ImmutableSet<Error> errors) {
        return new PrivilegesEvaluationResult(this.status, reason, this.availableIndices, indexToActionPrivilegeTable, errors);
    }
    
    public PrivilegesEvaluationResult availableIndices(ImmutableSet<String> availableIndices, CheckTable<String, Action> indexToActionPrivilegeTable) {
        return new PrivilegesEvaluationResult(this.status, this.reason, availableIndices,  indexToActionPrivilegeTable, errors);
    }

    public PrivilegesEvaluationResult status(Status status) {
        return new PrivilegesEvaluationResult(status, this.reason, this.availableIndices, this.indexToActionPrivilegeTable, this.errors);
    }
    
    public CheckTable<String, Action> getIndexToActionPrivilegeTable() {
        return indexToActionPrivilegeTable;
    }

    public ImmutableSet<Error> getErrors() {
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

        public Error(String message, Throwable cause) {
            this.message = message;
            this.cause = cause;
        }

        public String getMessage() {
            return message;
        }

        public Throwable getCause() {
            return cause;
        }

        @Override
        public String toString() {
            if (cause != null) {
                return message + " [" + cause + "]";
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
    }

}
