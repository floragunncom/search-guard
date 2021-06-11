package com.floragunn.signals.execution;

import com.floragunn.signals.watch.result.WatchLog;

public class WatchExecutionException extends Exception {
    private static final long serialVersionUID = 3454171384248735178L;

    private final WatchLog watchLog;

    public WatchExecutionException(String message, Throwable cause, WatchLog watchLog) {
        super(message, cause);
        this.watchLog = watchLog;
    }

    public WatchExecutionException(String message, WatchLog watchLog) {
        super(message);
        this.watchLog = watchLog;

    }

    public WatchExecutionException(Throwable cause, WatchLog watchLog) {
        super(cause);
        this.watchLog = watchLog;

    }

    public WatchLog getWatchLog() {
        return watchLog;
    }

}
