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
package com.floragunn.searchsupport.junit;

import com.google.common.base.Strings;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class LoggingTestWatcher extends TestWatcher {

    private static final int LINE_WIDTH = 100;
    private static final String DIV = "\n" + Strings.repeat("-", LINE_WIDTH) + "\n";

    @Override
    protected void succeeded(Description description) {
        System.out.println(DIV + AnsiColor.BLACK_ON_GREEN + Strings.padEnd("SUCCESS: " + description, LINE_WIDTH, ' ') + AnsiColor.RESET + DIV);
    }

    @Override
    protected void failed(Throwable e, Description description) {
        System.out.println(DIV + AnsiColor.BLACK_ON_RED + Strings.padEnd("FAILED: " + description, LINE_WIDTH, ' ') + AnsiColor.RESET + DIV);
        e.printStackTrace(System.out);
        System.out.println(DIV);
    }

    @Override
    protected void starting(Description description) {
        System.out.println(DIV + "Starting: " + description + DIV);
    }

    @Override
    protected void finished(Description description) {
    }

};
