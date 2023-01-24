/*
 * Copyright 2021 floragunn GmbH
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
package com.floragunn.searchsupport.junit;

import java.time.Duration;
import org.junit.Assert;

public class AsyncAssert {

    public static void awaitAssert(String message, AssertSupplier condition, Duration maxWaitingTime) throws Exception {
        long timeout = System.currentTimeMillis() + maxWaitingTime.toMillis();
        Exception lastException;

        do {
            try {
                if (condition.get()) {
                    return;
                }

                lastException = null;
            } catch (Exception e) {
                lastException = e;
            }

            sleep(50);
        } while (timeout >= System.currentTimeMillis());

        if (lastException == null) {
            Assert.fail(message);
        } else {
            throw lastException;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @FunctionalInterface
    public interface AssertSupplier {
        boolean get() throws Exception;
    }

}
