package com.floragunn.searchguard.test.helper.utils;

public class UnitTestForkNumberProvider {

    public static int getUnitTestForkNumber() {
        String forkno = System.getProperty("forkno");

        if (forkno != null && forkno.length() > 0) {
            return Integer.parseInt(forkno.split("_")[1]);
        } else {
            return 42;
        }
    }
}
