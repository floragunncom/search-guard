package com.floragunn.signals.watch.action.handlers;

public interface AutoResolveActionHandler {
    public boolean isAutoResolveEnabled();

    public ActionHandler getResolveActionHandler();
}
