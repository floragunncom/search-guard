package com.floragunn.searchsupport.util;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class CheckTableTest {

    @Test
    public void test() {
        Set<String> rows = ImmutableSet.of("esb-prod-1", "esb-prod-2", "esb-prod-3", "esb-prod-4", "esb-prod-5", ".monitoring-es-7-2021.12.30",
                ".async-search", "humanresources", "finance", "suggest", "nested", "logs", "auditlog-2021.12.30");

        Set<String> columns = ImmutableSet.of("indices:admin/settings/update");

        CheckTable<String, String> checkTable = CheckTable.create(rows, columns);

        checkTable.checkIf(rows, (action) -> true);

        System.out.println(checkTable);

        Assert.assertTrue(checkTable.toString(), checkTable.isComplete());
    }
}
