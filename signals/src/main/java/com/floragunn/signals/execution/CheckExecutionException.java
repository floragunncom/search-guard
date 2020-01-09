package com.floragunn.signals.execution;

import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.result.ErrorInfo;

public class CheckExecutionException extends WatchOperationExecutionException {

    private static final long serialVersionUID = 9090226882137384236L;

    private final String checkId;

    public CheckExecutionException(Check check, String message, Throwable cause) {
        super(message, cause);
        this.checkId = check.toString();
    }

    public CheckExecutionException(Check check, String message) {
        super(message);
        this.checkId = check.toString();
    }

    public String getCheckId() {
        return checkId;
    }

    public ErrorInfo toErrorInfo() {
        ErrorInfo result = super.toErrorInfo();

        result.setCheck(checkId);

        return result;
    }

}
