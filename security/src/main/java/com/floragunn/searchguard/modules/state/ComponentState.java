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

package com.floragunn.searchguard.modules.state;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.jar.Manifest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.searchguard.configuration.SearchGuardLicense;
import com.floragunn.searchsupport.json.BasicJsonReader;
import com.floragunn.searchsupport.json.BasicJsonWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;

public class ComponentState implements Writeable, ToXContentObject {
    private static final Logger log = LogManager.getLogger(ComponentState.class);

    private final String type;
    private final String name;
    private String className;
    private volatile State state = State.INITIALIZING;
    private volatile String subState;
    private volatile int tries;
    private volatile String message;
    private volatile String detailJson;
    private volatile List<Object> detailJsonElements;
    private volatile Throwable initException;
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
    private SearchGuardLicense license;
    private boolean enterprise;
    private int sortPrio;
    private String configVersion;
    private String configJson;
    private Map<String, Object> metrics;

    private List<ComponentState> parts = new ArrayList<>();

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

    public ComponentState(StreamInput in) throws IOException {
        this.type = in.readOptionalString();
        this.name = in.readString();
        this.className = in.readOptionalString();
        this.sortPrio = in.readInt();
        this.nodeId = in.readOptionalString();
        this.nodeName = in.readOptionalString();
        @SuppressWarnings("unused")
        int version = in.readInt();
        this.state = in.readEnum(State.class);
        this.subState = in.readOptionalString();
        this.tries = in.readInt();
        this.message = in.readOptionalString();
        this.detailJson = in.readOptionalString();
        this.startedAt = in.readOptionalInstant();
        this.initializedAt = in.readOptionalInstant();
        this.changedAt = in.readOptionalInstant();
        this.failedAt = in.readOptionalInstant();
        this.nextTryAt = in.readOptionalInstant();
        this.mandatory = in.readBoolean();
        this.jarFileName = in.readOptionalString();
        this.jarVersion = in.readOptionalString();
        this.jarBuildTime = in.readOptionalString();

        if (in.readBoolean()) {
            this.initException = in.readException();
        }

        this.lastExceptions = in.readMap(StreamInput::readString, ExceptionRecord::new);

        if (in.readBoolean()) {
            this.license = new SearchGuardLicense(in);
        }

        this.enterprise = in.readBoolean();
        this.configVersion = in.readOptionalString();
        this.configJson = in.readOptionalString();

        if (in.readBoolean()) {
            this.metrics = in.readMap();
        }

        this.parts = in.readList(ComponentState::new);
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

    public String getSortingKey() {
        return Strings.padStart(String.valueOf(sortPrio), 10, '0') + "::" + getKey();
    }

    public enum State {
        INITIALIZING, INITIALIZED, PARTIALLY_INITIALIZED, FAILED, DISABLED, SUSPENDED
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(type);
        out.writeString(name);
        out.writeOptionalString(className);
        out.writeInt(sortPrio);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(nodeName);
        out.writeInt(1);
        out.writeEnum(state);
        out.writeOptionalString(subState);
        out.writeInt(this.tries);
        out.writeOptionalString(this.message);
        out.writeOptionalString(this.detailJson);
        out.writeOptionalInstant(this.startedAt);
        out.writeOptionalInstant(this.initializedAt);
        out.writeOptionalInstant(this.changedAt);
        out.writeOptionalInstant(this.failedAt);
        out.writeOptionalInstant(this.nextTryAt);
        out.writeBoolean(mandatory);
        out.writeOptionalString(this.jarFileName);
        out.writeOptionalString(this.jarVersion);
        out.writeOptionalString(this.jarBuildTime);

        if (this.initException != null) {
            out.writeBoolean(true);
            out.writeException(this.initException);
        } else {
            out.writeBoolean(false);
        }

        out.writeMap(this.lastExceptions, StreamOutput::writeString, (o, v) -> v.writeTo(o));

        if (this.license != null) {
            out.writeBoolean(true);
            this.license.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(enterprise);
        out.writeOptionalString(configVersion);
        out.writeOptionalString(configJson);

        if (this.metrics != null) {
            out.writeBoolean(true);
            out.writeMap(this.metrics);
        } else {
            out.writeBoolean(false);
        }

        out.writeList(parts);

    }

    public PartsStats updateStateFromParts() {
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

    public String getDetailJson() {
        if (detailJson != null) {
            return detailJson;
        } else if (detailJsonElements != null) {
            try {
                return BasicJsonWriter.writeAsString(detailJsonElements);
            } catch (Exception e) {
                log.error("Error while writing " + detailJsonElements, e);
                return null;
            }
        } else {
            return null;
        }
    }

    public void setDetailJson(String detailJson) {
        this.detailJson = detailJson;
    }

    public Throwable getInitException() {
        return initException;
    }

    public void setFailed(Throwable initException) {
        Instant now = Instant.now();
        this.initException = initException;
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

    public void setInitException(Exception initException) {
        this.initException = initException;
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
        if (detailJsonElements == null) {
            if (detailJson != null) {
                try {
                    Object parsedDetailJson = BasicJsonReader.read(detailJson);

                    if (parsedDetailJson instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = (List<Object>) parsedDetailJson;
                        detailJsonElements = new ArrayList<>(list);
                    } else if (parsedDetailJson != null) {
                        detailJsonElements = new ArrayList<>();
                        detailJsonElements.add(parsedDetailJson);
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error while parsing detail JSON", e);
                }

                detailJson = null;
            }

            if (detailJsonElements == null) {
                detailJsonElements = new ArrayList<>();
            }
        }

        detailJsonElements.add(detail);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (nodeId != null) {
            builder.field("node_id", nodeId);
        }

        if (nodeName != null) {
            builder.field("node_name", nodeName);
        }

        if (type != null) {
            builder.field("type", type);
        }

        builder.field("name", name);
        builder.field("state", state);

        if (subState != null) {
            builder.field("sub_state", subState);
        }

        if (message != null) {
            builder.field("message", message);
        }

        if (detailJson != null) {
            builder.rawField("detail", new ByteArrayInputStream(detailJson.getBytes(Charsets.UTF_8)), XContentType.JSON);
        }

        if (startedAt != null) {
            builder.field("started_at", startedAt.toString());
        }

        if (changedAt != null && (startedAt == null || changedAt.isAfter(startedAt))) {
            builder.field("changed_at", changedAt.toString());
        }

        if (initializedAt != null) {
            builder.field("initialized_at", initializedAt.toString());
        }

        if (failedAt != null) {
            builder.field("failed_at", failedAt.toString());
        }

        if (nextTryAt != null) {
            builder.field("next_try_at", nextTryAt.toString());
        }

        if (initException != null) {
            builder.field("init_exception", exceptionToString(initException));
        }

        if (jarFileName != null || jarVersion != null || jarBuildTime != null) {
            builder.startObject("build");

            if (jarFileName != null) {
                builder.field("file", jarFileName);
            }

            if (jarVersion != null) {
                builder.field("version", jarVersion);
            }

            if (jarBuildTime != null) {
                builder.field("build_time", jarBuildTime);
            }

            builder.endObject();
        }

        if (configVersion != null || configJson != null) {
            builder.startObject("config");

            if (configVersion != null) {
                builder.field("version", configVersion);
            }

            if (configJson != null) {
                builder.rawField("content", new ByteArrayInputStream(configJson.getBytes(Charsets.UTF_8)), XContentType.JSON);
            }
            builder.endObject();
        }

        if (parts.size() > 0) {
            builder.field("parts", parts);
        }


        if (lastExceptions.size() != 0) {
            builder.field("last_exceptions", lastExceptions);
        }

        
        builder.endObject();

        return builder;
    }

    private static List<String> exceptionToString(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        return Arrays.asList(stringWriter.toString().replace('\t', ' ').split("\n"));
    }

    public List<ComponentState> getParts() {
        return Collections.unmodifiableList(parts);
    }

    public void addPart(ComponentState moduleState) {
        parts.add(moduleState);
    }

    public ComponentState getPart(String type, String name) {
        for (ComponentState part : parts) {
            if (name.equals(part.getName()) && Objects.equals(type, part.type)) {
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

    public void replacePart(ComponentState part) {
        ComponentState existing = getPart(part.getType(), part.getName());

        if (existing != null) {
            parts.remove(existing);
        }

        parts.add(part);
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
        Instant result = this.startedAt;

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

    public SearchGuardLicense getLicense() {
        return license;
    }

    public void setLicense(SearchGuardLicense license) {
        this.license = license;
    }

    public boolean isEnterprise() {
        return enterprise;
    }

    public void setEnterprise(boolean enterprise) {
        this.enterprise = enterprise;
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

    public String getConfigJson() {
        return configJson;
    }

    public void setConfigJson(String configJson) {
        this.configJson = configJson;
    }

    public static class ExceptionRecord implements Writeable, ToXContentObject {
        private final Throwable exception;
        private final String message;
        private final Instant occuredAt;

        public ExceptionRecord(Throwable exception, String message) {
            this.exception = exception;
            this.message = message;
            this.occuredAt = Instant.now();
        }

        public ExceptionRecord(StreamInput in) throws IOException {
            this.exception = in.readException();
            this.occuredAt = in.readInstant();
            this.message = in.readOptionalString();
        }

        public ExceptionRecord(Throwable exception) {
            this(exception, null);
        }

        public Throwable getException() {
            return exception;
        }

        public String getMessage() {
            return message;
        }

        public Instant getOccuredAt() {
            return occuredAt;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeException(this.exception);
            out.writeInstant(this.occuredAt);
            out.writeOptionalString(this.message);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (message != null) {
                builder.field("message", message);
            }
            builder.field("exception", exceptionToString(exception));
            builder.field("occured_at", occuredAt.toString());

            

            builder.endObject();
            return builder;
        }
    }

    @Override
    public String toString() {
        return "ComponentState [type=" + type + ", name=" + name + ", className=" + className + ", state=" + state + ", subState=" + subState
                + ", tries=" + tries + ", message=" + message + ", detailJson=" + detailJson + ", detailJsonElements=" + detailJsonElements
                + ", initException=" + initException + ", startedAt=" + startedAt + ", initializedAt=" + initializedAt + ", changedAt=" + changedAt
                + ", failedAt=" + failedAt + ", nextTryAt=" + nextTryAt + ", lastExceptions=" + lastExceptions + ", mandatory=" + mandatory
                + ", jarFileName=" + jarFileName + ", jarVersion=" + jarVersion + ", jarBuildTime=" + jarBuildTime + ", nodeId=" + nodeId
                + ", nodeName=" + nodeName + ", license=" + license + ", enterprise=" + enterprise + ", sortPrio=" + sortPrio + ", configVersion="
                + configVersion + ", configJson=" + configJson + ", metrics=" + metrics + ", parts=" + parts + "]";
    }
}
