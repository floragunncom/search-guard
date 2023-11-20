/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchsupport.cstate;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.jar.Manifest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchsupport.cstate.metrics.Measurement;
import com.google.common.base.Strings;

public class ComponentState implements Document<ComponentState> {
    private static final Logger log = LogManager.getLogger(ComponentState.class);

    private final String type;
    private final String name;
    private String className;
    private volatile State state = State.INITIALIZING;
    private volatile String subState;
    private volatile int tries;
    private volatile String message;
    private List<Object> detailJsonElements = Collections.synchronizedList(new ArrayList<>());
    private volatile ImmutableList<String> initException;
    private Instant startedAt = Instant.now();
    private volatile Instant initializedAt;
    private volatile Instant changedAt;
    private Instant failedAt;
    private volatile Instant nextTryAt;
    private Map<String, ExceptionRecord> lastExceptions = new ConcurrentHashMap<>();
    private boolean mandatory = true;
    private String jarFileName;
    private String jarVersion;
    private String jarBuildTime;
    private String nodeId;
    private String nodeName;
    private byte licenseRequired;
    private int sortPrio;
    private String configVersion;
    private ImmutableMap<String, Measurement<?>> metrics = ImmutableMap.empty();
    private ImmutableMap<String, Object> moreConfigProperties;

    private List<ComponentState> parts = new ArrayList<>();

    public ComponentState(String name) {
        this.type = null;
        this.name = name;
        this.sortPrio = 0;
    }

    public ComponentState(int sortPrio, String type, String name) {
        this.type = type;
        this.name = name;
        this.sortPrio = sortPrio;
    }

    public ComponentState(int sortPrio, String type, String name, Class<?> referenceClass) {
        this.type = type;
        this.name = name;
        this.sortPrio = sortPrio;

        if (referenceClass != null) {
            setReferenceClass(referenceClass);
        }
    }

    public ComponentState(DocNode docNode) {
        this.type = docNode.getAsString("type");
        this.name = docNode.getAsString("name");
        this.className = docNode.getAsString("class_name");
        this.nodeId = docNode.getAsString("node_id");
        this.nodeName = docNode.getAsString("node_name");
        this.state = docNode.hasNonNull("state") ? State.valueOf(docNode.getAsString("state")) : null;
        this.subState = docNode.getAsString("sub_state");
        try {
            this.tries = docNode.hasNonNull("tries") ? docNode.getNumber("tries").intValue() : 0;
        } catch (ConfigValidationException e) {
            log.error("Invalid value for tries", e);
        }
        this.message = docNode.getAsString("message");

        if (docNode.hasNonNull("detail")) {
            Object detail = docNode.get("detail");

            if (detail instanceof List) {
                this.detailJsonElements.addAll((List<?>) detail);
            } else {
                this.detailJsonElements.add(detail);
            }
        }

        this.startedAt = docNode.hasNonNull("started_at") ? Instant.parse(docNode.getAsString("started_at")) : null;
        this.initializedAt = docNode.hasNonNull("initialized_at") ? Instant.parse(docNode.getAsString("initialized_at")) : null;
        this.changedAt = docNode.hasNonNull("changed_at") ? Instant.parse(docNode.getAsString("changed_at")) : null;
        this.failedAt = docNode.hasNonNull("failed_at") ? Instant.parse(docNode.getAsString("failed_at")) : null;
        this.nextTryAt = docNode.hasNonNull("next_try_at") ? Instant.parse(docNode.getAsString("next_try_at")) : null;
        this.mandatory = docNode.get("mandatory") instanceof Boolean ? (Boolean) docNode.get("mandatory") : false;        

        if (docNode.hasNonNull("build")) {
            DocNode build = docNode.getAsNode("build");
            this.jarFileName = build.getAsString("file");
            this.jarVersion = build.getAsString("version");
            this.jarBuildTime = build.getAsString("time");
        }

        this.initException = docNode.getAsListOfStrings("init_exception");

        if (docNode.hasNonNull("last_exceptions")) {
            DocNode lastExceptions = docNode.getAsNode("last_exceptions");

            for (String key : lastExceptions.keySet()) {
                this.lastExceptions.put(key, new ExceptionRecord(lastExceptions.getAsNode(key)));
            }
        }

        if (docNode.hasNonNull("config")) {
            DocNode config = docNode.getAsNode("config");
            ImmutableMap.Builder<String, Object> moreConfigProperties = new ImmutableMap.Builder<>();

            for (String key : config.keySet()) {

                if (key.equals("version")) {
                    this.configVersion = config.getAsString("version");
                } else {
                    moreConfigProperties.put(key, config.get(key));
                }
            }

            this.moreConfigProperties = moreConfigProperties.build();
        }

        if (docNode.hasNonNull("metrics")) {
            DocNode metricsNode = docNode.getAsNode("metrics");
            ImmutableMap.Builder<String, Measurement<?>> metrics = new ImmutableMap.Builder<>(metricsNode.size());

            for (String key : metricsNode.keySet()) {
                metrics.put(key, Measurement.parse(metricsNode.getAsNode(key)));
            }

            this.metrics = metrics.build();
        }

        if (docNode.hasNonNull("parts")) {
            for (DocNode part : docNode.getAsListOfNodes("parts")) {
                this.parts.add(new ComponentState(part));
            }
        }
    }

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public String getKey() {
        return this.type + "::" + this.name;
    }

    public String getTypeAndName() {
        if (this.type == null) {
            return this.name;
        } else {
            return this.type + "/" + this.name;
        }
    }

    public String getSortingKey() {
        return Strings.padStart(String.valueOf(sortPrio), 10, '0') + "::" + getKey();
    }

    public enum State {
        INITIALIZING, INITIALIZED, PARTIALLY_INITIALIZED, FAILED, DISABLED, SUSPENDED
    }

    public PartsStats updateStateFromParts() {
        try {
            int total = parts.size();
            PartsStats result = new PartsStats();

            if (total == 0) {
                return result;
            }

            for (ComponentState part : parts) {
                part.updateStateFromParts();
            }

            int failed = 0;
            int initialized = 0;
            int disabled = 0;
            int initializing = 0;
            int mandatory = 0;
            Instant lastInitialized = this.initializedAt;
            Instant lastFailed = this.failedAt;
            Instant lastChanged = this.changedAt;

            for (ComponentState part : parts) {
                lastChanged = max(lastChanged, part.changedAt);

                if (part.state == State.INITIALIZING) {
                    initializing++;
                }

                if (!part.isMandatory()) {
                    continue;
                }

                mandatory++;

                switch (part.getState()) {
                case INITIALIZED:
                    initialized++;
                    lastInitialized = max(lastInitialized, part.getInitializedAt());
                    break;
                case FAILED:
                    failed++;
                    lastFailed = max(lastFailed, part.getFailedAt());
                    break;
                case DISABLED:
                    disabled++;
                    break;
                case INITIALIZING:
                    break;
                case SUSPENDED:
                    initialized++;
                    break;
                case PARTIALLY_INITIALIZED:
                    break;

                }
            }

            if (initializing > 0) {
                setState(State.INITIALIZING);
            } else if (initialized == mandatory) {
                setInitialized();
                this.initializedAt = lastInitialized;
            } else if (disabled == mandatory) {
                setState(State.DISABLED);
            } else if (failed > 0) {
                setState(State.FAILED);
                this.failedAt = lastFailed;
            } else if (failed == 0) {
                setInitialized();
                this.initializedAt = lastInitialized;
            }

            this.changedAt = lastChanged;

            result.setTotal(total);
            result.setFailed(failed);
            result.setInitialized(initialized);
            result.setInitializing(initializing);
            result.setMandatory(mandatory);

            return result;
        } catch (Exception e) {
            log.error("Error in updateStateFromParts()", e);
            return new PartsStats();
        }

    }

    public static class PartsStats {
        private int total = 0;
        private int failed = 0;
        private int initialized = 0;
        private int disabled = 0;
        private int initializing = 0;
        private int mandatory = 0;

        public int getFailed() {
            return failed;
        }

        public void setFailed(int failed) {
            this.failed = failed;
        }

        public int getInitialized() {
            return initialized;
        }

        public void setInitialized(int initialized) {
            this.initialized = initialized;
        }

        public int getDisabled() {
            return disabled;
        }

        public void setDisabled(int disabled) {
            this.disabled = disabled;
        }

        public int getInitializing() {
            return initializing;
        }

        public void setInitializing(int initializing) {
            this.initializing = initializing;
        }

        public int getMandatory() {
            return mandatory;
        }

        public void setMandatory(int mandatory) {
            this.mandatory = mandatory;
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }

    }

    public State getState() {
        return state;
    }

    public boolean isFailed() {
        return state == State.FAILED;
    }

    public boolean isInitialized() {
        return state == State.INITIALIZED;
    }

    public void setState(State state) {
        if (state.equals(this.state)) {
            return;
        }

        this.state = state;
        this.changedAt = Instant.now();
        this.subState = null;
        this.tries = 0;
    }

    public void setState(State state, String subState) {
        if (state.equals(this.state) && Objects.equals(subState, this.subState)) {
            return;
        }

        this.state = state;
        this.subState = subState;
        this.changedAt = Instant.now();
        this.tries = 0;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setFailed(Throwable initException) {
        Instant now = Instant.now();
        this.initException = exceptionToString(initException);
        this.state = State.FAILED;
        this.subState = null;
        this.failedAt = now;
        this.changedAt = now;

    }

    public void setFailed(String message) {
        Instant now = Instant.now();
        this.message = message;
        this.state = State.FAILED;
        this.subState = null;
        this.failedAt = now;
        this.changedAt = now;
    }

    public void setInitialized() {
        Instant now = Instant.now();
        this.state = State.INITIALIZED;
        this.subState = null;
        this.initializedAt = now;
        this.changedAt = now;
    }

    public ComponentState initialized() {
        setInitialized();
        return this;
    }

    public void setInitException(Exception initException) {
        this.initException = exceptionToString(initException);
    }

    public Map<String, ExceptionRecord> getLastExceptions() {
        return lastExceptions;
    }

    public void addLastException(String key, ExceptionRecord exceptionRecord) {
        lastExceptions.put(key, exceptionRecord);
    }

    public void addLastException(String key, Throwable t) {
        addLastException(key, new ExceptionRecord(t));
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getInitializedAt() {
        return initializedAt;
    }

    public void setInitializedAt(Instant initializedAt) {
        this.initializedAt = initializedAt;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }

    public Instant getNextTryAt() {
        return nextTryAt;
    }

    public void setNextTryAt(Instant nextTryAt) {
        this.nextTryAt = nextTryAt;
    }

    public String getSubState() {
        return subState;
    }

    public void setSubState(String subState) {
        this.subState = subState;
    }

    public void addDetail(Object detail) {
        detailJsonElements.add(detail);
    }
    
    public void setDetailJson(String detailJson) {
        try {
            Object parsedDetailJson = DocReader.json().read(detailJson);
            detailJsonElements.clear();
            detailJsonElements.add(parsedDetailJson);
        } catch (DocumentParseException e) {
            log.error("Error while parsing detail JSON\n" + detailJson, e);
        }        
    }

    @Override
    public Object toBasicObject() {
        OrderedImmutableMap.Builder<String, Object> result = new OrderedImmutableMap.Builder<>(40);

        if (nodeId != null) {
            result.put("node_id", nodeId);
        }

        if (nodeName != null) {
            result.put("node_name", nodeName);
        }

        if (type != null) {
            result.put("type", type);
        }

        result.put("name", name);
        result.put("state", state);

        if (subState != null) {
            result.put("sub_state", subState);
        }

        if (message != null) {
            result.put("message", message);
        }

        if (detailJsonElements != null && detailJsonElements.size() != 0) {
            if (detailJsonElements.size() == 1) {
                result.put("detail", detailJsonElements.get(0));
            } else {
                result.put("detail", detailJsonElements);
            }
        }

        if (startedAt != null) {
            result.put("started_at", startedAt.toString());
        }

        if (changedAt != null && (startedAt == null || changedAt.isAfter(startedAt))) {
            result.put("changed_at", changedAt.toString());
        }

        if (initializedAt != null) {
            result.put("initialized_at", initializedAt.toString());
        }

        if (failedAt != null) {
            result.put("failed_at", failedAt.toString());
        }

        if (nextTryAt != null) {
            result.put("next_try_at", nextTryAt.toString());
        }

        if (initException != null) {
            result.put("init_exception", initException);
        }

        if (licenseRequired != 0) {
            result.put("license_required", getLicenseRequiredInfo());
        }

        if (jarFileName != null || jarVersion != null || jarBuildTime != null) {
            result.put("build", OrderedImmutableMap.ofNonNull("file", jarFileName, "version", jarVersion, "time", jarBuildTime));
        }

        if (configVersion != null || (moreConfigProperties != null && !moreConfigProperties.isEmpty())) {
            OrderedImmutableMap.Builder<String, Object> config = new OrderedImmutableMap.Builder<>();

            if (configVersion != null) {
                config.put("version", configVersion);
            }

            if (moreConfigProperties != null && !moreConfigProperties.isEmpty()) {
                for (Map.Entry<String, Object> entry : moreConfigProperties.entrySet()) {
                    config.put(entry.getKey(), entry.getValue());
                }
            }

            result.put("config", config.build());
        }

        if (metrics != null && metrics.size() != 0) {
            result.put("metrics", metrics.mapValues((v) -> OrderedImmutableMap.of(v.getType(), v)));
        }

        if (parts.size() > 0) {
            result.put("parts", parts);
        }

        if (lastExceptions.size() != 0) {
            result.put("last_exceptions", lastExceptions);
        }

        return result.build();
    }

    private static ImmutableList<String> exceptionToString(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        return ImmutableList.ofArray(stringWriter.toString().replace('\t', ' ').split("\n"));
    }

    public List<ComponentState> getParts() {
        return Collections.unmodifiableList(parts);
    }

    public void addPart(ComponentState moduleState) {
        parts.add(moduleState);
    }

    public void addParts(ComponentState... parts) {
        for (ComponentState part : parts) {
            this.parts.add(part);
        }
    }

    public ComponentState getPart(String type, String name) {
        for (ComponentState part : parts) {
            if (Objects.equals(name, part.getName()) && Objects.equals(type, part.type)) {
                return part;
            }
        }

        return null;
    }

    public ComponentState getOrCreatePart(String type, String name) {
        ComponentState part = getPart(type, name);

        if (part == null) {
            part = new ComponentState(0, type, name);
            parts.add(part);
        }

        return part;
    }

    public synchronized void replacePart(ComponentState part) {
        ComponentState existing = getPart(part.getType(), part.getName());

        if (existing != null) {
            parts.remove(existing);
        }

        parts.add(part);
    }
    
    public synchronized void clearParts() {
        parts.clear();
    }

    public synchronized void replacePartsWithType(String type, ComponentState newPart) {
        Iterator<ComponentState> iter = this.parts.iterator();

        while (iter.hasNext()) {
            ComponentState part = iter.next();

            if (type.equals(part.getType())) {
                iter.remove();
            }
        }

        parts.add(newPart);
    }

    public synchronized void replacePartsWithType(String type, Collection<ComponentState> newParts) {
        Iterator<ComponentState> iter = this.parts.iterator();

        while (iter.hasNext()) {
            ComponentState part = iter.next();

            if (type.equals(part.getType())) {
                iter.remove();
            }
        }

        parts.addAll(newParts);
    }

    public void addMetrics(String key, Measurement<?> measurement) {
        this.metrics = this.metrics.with(key, measurement);
    }

    public void addMetrics(String key1, Measurement<?> measurement1, String key2, Measurement<?> measurement2) {
        this.metrics = this.metrics.with(ImmutableMap.of(key1, measurement1, key2, measurement2));
    }

    public void addMetrics(String key1, Measurement<?> measurement1, String key2, Measurement<?> measurement2, String key3,
            Measurement<?> measurement3) {
        this.metrics = this.metrics.with(ImmutableMap.of(key1, measurement1, key2, measurement2, key3, measurement3));
    }
    
    public void addMetrics(String key1, Measurement<?> measurement1, String key2, Measurement<?> measurement2, String key3,
            Measurement<?> measurement3, String key4, Measurement<?> measurement4) {
        this.metrics = this.metrics.with(ImmutableMap.of(key1, measurement1, key2, measurement2, key3, measurement3, key4, measurement4));
    }

    public void addMetrics(Map<String, Measurement<?>> measurements) {
        this.metrics = this.metrics.with(ImmutableMap.of(measurements));
    }

    public void setConfigProperty(String property, Object value) {
        if (this.moreConfigProperties == null) {
            this.moreConfigProperties = ImmutableMap.of(property, value);
        } else {
            this.moreConfigProperties = this.moreConfigProperties.with(property, value);
        }
    }

    public int getTries() {
        return tries;
    }

    public void startNextTry() {
        this.tries++;
        this.changedAt = Instant.now();
    }

    private static Instant max(Instant i1, Instant i2) {
        if (i1 == null) {
            return i2;
        }

        if (i2 == null) {
            return i1;
        }

        if (i1.isAfter(i2)) {
            return i1;
        } else {
            return i2;
        }
    }

    private static Instant min(Instant i1, Instant i2) {
        if (i1 == null) {
            return i2;
        }

        if (i2 == null) {
            return i1;
        }

        if (i1.isBefore(i2)) {
            return i1;
        } else {
            return i2;
        }
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    public ComponentState mandatory(boolean mandatory) {
        this.mandatory = mandatory;
        return this;
    }

    public String getJarFileName() {
        return jarFileName;
    }

    public String getJarVersion() {
        return jarVersion;
    }

    public String getJarBuildTime() {
        return jarBuildTime;
    }

    public Instant getMinStartForInitializingState() {
        Instant result = this.state == ComponentState.State.INITIALIZING ? this.startedAt : null;

        for (ComponentState part : parts) {
            Instant start = part.getMinStartForInitializingState();

            result = min(result, start);
        }

        return result;
    }

    public void setReferenceClass(Class<?> referenceClass) {
        try {
            this.className = referenceClass.getName();

            URL locationInJarUrl = referenceClass.getResource("/" + referenceClass.getName().replace(".", "/") + ".class");

            if (locationInJarUrl == null) {
                return;
            }

            String locationInJar = locationInJarUrl.toString();
            String jarFilePath = getJarFilePath(locationInJar);

            if (jarFilePath != null) {
                this.jarFileName = getFilename(jarFilePath);

                try {
                    Manifest manifest = getManifest(jarFilePath);

                    this.jarVersion = manifest.getMainAttributes().getValue("Implementation-Version");
                    this.jarBuildTime = manifest.getMainAttributes().getValue("Build-Time");
                } catch (Exception e) {
                    log.error("Error while reading manifest " + jarFilePath, e);
                }
            } else {
                this.jarFileName = locationInJar;
            }
        } catch (Throwable e) {
            log.error("Error while getting class info for " + referenceClass, e);
        }
    }

    private String getFilename(String url) {
        int slash = url.lastIndexOf('/');

        if (slash != -1) {
            return url.substring(slash + 1);
        } else {
            return url;
        }
    }

    private Manifest getManifest(String jarFilePath) throws MalformedURLException, IOException {

        String manifestPath = jarFilePath + "!/META-INF/MANIFEST.MF";

        try (InputStream stream = new URL(manifestPath).openStream()) {
            return new Manifest(stream);
        }
    }

    private String getJarFilePath(String locationInJar) {
        if (!locationInJar.startsWith("jar:")) {
            return null;
        }

        int index = locationInJar.lastIndexOf('!');

        if (index != -1) {
            return locationInJar.substring(0, index);
        } else {
            return null;
        }
    }

    public ComponentState findPart(Function<ComponentState, Boolean> predicate) {
        for (ComponentState part : parts) {
            if (predicate.apply(part)) {
                return part;
            }
        }

        return null;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String node) {
        this.nodeId = node;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getClassName() {
        return className;
    }

    public int getSortPrio() {
        return sortPrio;
    }

    public void setSortPrio(int sortPrio) {
        this.sortPrio = sortPrio;
    }

    public String getConfigVersion() {
        return configVersion;
    }

    public void setConfigVersion(String configVersion) {
        this.configVersion = configVersion;
    }

    public void setConfigVersion(long configVersion) {
        this.configVersion = Long.toString(configVersion);
    }

    public static class ExceptionRecord implements Document<ExceptionRecord> {
        private final ImmutableList<String> exception;
        private final String message;
        private final Instant occuredAt;

        public ExceptionRecord(Throwable exception, String message) {
            this.exception = exceptionToString(exception);
            this.message = message;
            this.occuredAt = Instant.now();
        }

        public ExceptionRecord(DocNode docNode) {
            this.message = docNode.getAsString("message");
            this.exception = docNode.getAsListOfStrings("exception");
            this.occuredAt = docNode.hasNonNull("occured_at") ? Instant.parse(docNode.getAsString("occured_at")) : null;
        }

        public ExceptionRecord(Throwable exception) {
            this(exception, null);
        }

        public String getMessage() {
            return message;
        }

        public Instant getOccuredAt() {
            return occuredAt;
        }

        @Override
        public Object toBasicObject() {
            return OrderedImmutableMap.ofNonNull("message", message, "exception", exception, "occured_at", occuredAt);
        }
    }

    public byte getLicenseRequired() {
        return licenseRequired;
    }

    public String getLicenseRequiredInfo() {
        switch (licenseRequired) {
        case 0:
            return "no";
        case 1:
            return "enterprise";
        default:
            return String.valueOf(licenseRequired);
        }
    }

    public void setLicenseRequired(byte licenseRequired) {
        this.licenseRequired = licenseRequired;
    }

    public ComponentState requiresEnterpriseLicense() {
        this.licenseRequired = 1;
        return this;
    }

    public ImmutableMap<String, Measurement<?>> getMetrics() {
        return metrics;
    }

}
