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

package com.floragunn.searchguard.ssl.test.helper.network;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.floragunn.searchguard.ssl.test.helper.network.SocketUtils.SocketType;

public class PortAllocator {

    public static final PortAllocator TCP = new PortAllocator(SocketType.TCP, Duration.ofSeconds(100));
    public static final PortAllocator UDP = new PortAllocator(SocketType.UDP, Duration.ofSeconds(100));

    private final SocketType socketType;
    private final Duration timeoutDuration;
    private final Map<Integer, AllocatedPort> allocatedPorts = new HashMap<>();

    PortAllocator(SocketType socketType, Duration timeoutDuration) {
        this.socketType = socketType;
        this.timeoutDuration = timeoutDuration;
    }

    public SortedSet<Integer> allocate(String clientName, int numRequested, int minPort) {

        int startPort = minPort;

        while (!isAvailable(startPort)) {
            startPort += 10;
        }

        SortedSet<Integer> foundPorts = new TreeSet<>();

        for (int currentPort = startPort; foundPorts.size() < numRequested && currentPort < SocketUtils.PORT_RANGE_MAX
                && (currentPort - startPort) < 10000; currentPort++) {
            if (allocate(clientName, currentPort)) {
                foundPorts.add(currentPort);
            }
        }

        if (foundPorts.size() < numRequested) {
            throw new IllegalStateException("Could not find " + numRequested + " free ports starting at " + minPort + " for " + clientName);
        }

        return foundPorts;
    }

    public int allocateSingle(String clientName, int minPort) {

        int startPort = minPort;

        for (int currentPort = startPort; currentPort < SocketUtils.PORT_RANGE_MAX && (currentPort - startPort) < 10000; currentPort++) {
            if (allocate(clientName, currentPort)) {
                return currentPort;
            }
        }

        throw new IllegalStateException("Could not find free port starting at " + minPort + " for " + clientName);

    }

    public void blacklist(int... ports) {

        for (int port : ports) {
            allocate("blacklisted", port);
        }
    }

    private boolean isInUse(int port) {
        boolean result = !this.socketType.isPortAvailable(port);

        if (result) {
            synchronized (this) {
                allocatedPorts.put(port, new AllocatedPort("external"));
            }
        }

        return result;
    }

    private boolean isAvailable(int port) {
        return !isAllocated(port) && !isInUse(port);
    }

    private synchronized boolean isAllocated(int port) {
        AllocatedPort allocatedPort = this.allocatedPorts.get(port);

        return allocatedPort != null && !allocatedPort.isTimedOut();
    }

    private synchronized boolean allocate(String clientName, int port) {

        AllocatedPort allocatedPort = allocatedPorts.get(port);

        if (allocatedPort != null && allocatedPort.isTimedOut()) {
            allocatedPort = null;
            allocatedPorts.remove(port);
        }

        if (allocatedPort == null && !isInUse(port)) {
            allocatedPorts.put(port, new AllocatedPort(clientName));
            return true;
        } else {
            return false;
        }
    }

    private class AllocatedPort {
        final String client;
        final Instant allocatedAt;

        AllocatedPort(String client) {
            this.client = client;
            this.allocatedAt = Instant.now();
        }

        boolean isTimedOut() {
            return allocatedAt.plus(timeoutDuration).isBefore(Instant.now());
        }

        @Override
        public String toString() {
            return "AllocatedPort [client=" + client + ", allocatedAt=" + allocatedAt + "]";
        }
    }
}
