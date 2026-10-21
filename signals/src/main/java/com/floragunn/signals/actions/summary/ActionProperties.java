/*
 * Copyright 2023 floragunn GmbH
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

package com.floragunn.signals.actions.summary;

class ActionProperties {

    private final String checkResultName;
    private final Boolean checkResultValue;
    private final String errorName;
    private final String errorValue;
    private final String statusCodeName;
    private final String statusCodeValue;
    private final String statusDetailsName;
    private final String statusDetailsValue;

    public ActionProperties(String checkResultName, Boolean checkResultValue, String errorName, String errorValue,
                            String statusCodeName, String statusCodeValue, String statusDetailsName, String statusDetailsValue) {
        this.checkResultName = checkResultName;
        this.checkResultValue = checkResultValue;
        this.errorName = errorName;
        this.errorValue = errorValue;
        this.statusCodeName = statusCodeName;
        this.statusCodeValue = statusCodeValue;
        this.statusDetailsName = statusDetailsName;
        this.statusDetailsValue = statusDetailsValue;
    }

    public String getCheckResultName() {
        return checkResultName;
    }

    public Boolean getCheckResultValue() {
        return checkResultValue;
    }

    public String getErrorName() {
        return errorName;
    }

    public String getErrorValue() {
        return errorValue;
    }

    public String getStatusCodeName() {
        return statusCodeName;
    }

    public String getStatusCodeValue() {
        return statusCodeValue;
    }

    public String getStatusDetailsName() {
        return statusDetailsName;
    }

    public String getStatusDetailsValue() {
        return statusDetailsValue;
    }

    @Override public String toString() {
        return "ActionProperties{" + "checkResultName='" + checkResultName + '\'' + ", checkResultValue='" + checkResultValue + '\'' + ", errorName='" + errorName + '\'' + ", errorValue='" + errorValue + '\'' + ", statusCodeName='" + statusCodeName + '\'' + ", statusCodeValue='" + statusCodeValue + '\'' + ", statusDetailsName='" + statusDetailsName + '\'' + ", statusDetailsValue='" + statusDetailsValue + '\'' + '}';
    }
}
