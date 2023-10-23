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
