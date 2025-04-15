package com.floragunn.aim;

import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExternalProcessTest {
    private static LocalCluster CLUSTER;

    @BeforeAll
    public static void setup() {
        CLUSTER = new LocalCluster.Builder().singleNode().sslEnabled().enableModule(AutomatedIndexManagementModule.class).useExternalProcessCluster()
                .start();
    }

    @Test
    public void testExternalProcess() {
        Assertions.assertTrue(CLUSTER.isStarted());
    }
}
