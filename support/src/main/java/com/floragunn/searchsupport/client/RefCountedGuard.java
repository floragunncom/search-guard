package com.floragunn.searchsupport.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.RefCounted;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class RefCountedGuard<T extends RefCounted> implements AutoCloseable {

    private static final Logger log = LogManager.getLogger(RefCountedGuard.class);

    private final List<T> releasable = new LinkedList<>();

    public void add(T refCounted) {
        Objects.requireNonNull(refCounted, "Ref counted is required");
        releasable.add(refCounted);
        log.debug("Ref counted {} added.", refCounted);
    }

    public void release() {
        Iterator<T> iterator = releasable.iterator();
        while (iterator.hasNext()) {
            T refCounted = iterator.next();
            try {
                refCounted.decRef();
            } catch (Exception e) {
                log.error("Cannot release resource related to ref counted '{}'", refCounted, e);
            } finally {
                iterator.remove();
            }
        }
        log.debug("Release resources related to ref counted");
    }

    @Override
    public void close() {
        release();
        log.debug("Resources realised in close method");
    }
}
