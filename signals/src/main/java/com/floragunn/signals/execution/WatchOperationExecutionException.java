package com.floragunn.signals.execution;

import org.elasticsearch.script.ScriptException;
import org.elasticsearch.xcontent.ToXContent;

import com.floragunn.signals.watch.result.ErrorInfo;

public class WatchOperationExecutionException extends Exception {

    private static final long serialVersionUID = 4053120908207894686L;

    public WatchOperationExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public WatchOperationExecutionException(String message) {
        super(message);
    }

    public WatchOperationExecutionException(Throwable cause) {
        super(cause);
    }

    public ErrorInfo toErrorInfo() {
        Throwable cause = getCause();

        if (cause instanceof ScriptException) {
            if ("runtime error".equalsIgnoreCase(cause.getMessage()) && cause.getCause() != null) {
                return new ErrorInfo(null, constructMessage(cause.getCause()), (ToXContent) cause);
            } else {
                return new ErrorInfo(null, constructMessage(this), (ToXContent) cause);
            }
        } else {
            return new ErrorInfo(null, constructMessage(this), findToXContentCause(cause));
        }
    }

    private static String constructMessage(Throwable throwable) {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < 10; i++) {
            String message = throwable.getMessage();

            if (message == null) {
                message = throwable.toString();
            }

            if (result.indexOf(message) == -1) {
                if (result.length() != 0) {
                    result.append(":\n");
                }

                result.append(message);
            }

            if (throwable.getCause() == throwable || throwable.getCause() == null) {
                break;
            }

            throwable = throwable.getCause();
        }

        return result.toString();
    }

    private static ToXContent findToXContentCause(Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        for (int i = 0; i < 10; i++) {

            if (throwable instanceof ToXContent) {
                return (ToXContent) throwable;
            }

            if (throwable.getCause() == throwable || throwable.getCause() == null) {
                break;
            }

            throwable = throwable.getCause();
        }

        return null;

    }
}
