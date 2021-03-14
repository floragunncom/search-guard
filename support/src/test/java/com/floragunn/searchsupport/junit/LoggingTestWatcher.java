package com.floragunn.searchsupport.junit;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Strings;

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
