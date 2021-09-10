package com.floragunn.signals.support;

import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseExecutionException;

public class LuckySisyphos {
    private static final Logger log = LogManager.getLogger(LuckySisyphos.class);

    public static <Result> Result tryHard(Supplier<Result> function) {
        long start = System.currentTimeMillis();
        Exception lastException = null;

        do {
            try {
                return function.get();
            } catch (SearchPhaseExecutionException e) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Got SearchPhaseExecutionException when executing " + function, e);
                    }

                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        } while (System.currentTimeMillis() < start + 5 * 60 * 1000);

        throw new RuntimeException("Giving up after 5 minutes of trying. Don't say that I did not wait long enough! ^^", lastException);
    }
}
