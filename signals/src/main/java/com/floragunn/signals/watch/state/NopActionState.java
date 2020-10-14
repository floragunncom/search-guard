package com.floragunn.signals.watch.state;

import com.floragunn.searchsupport.util.temporal.DurationExpression;
import com.floragunn.signals.watch.common.Ack;

public class NopActionState extends ActionState {

    @Override
    public synchronized BasicState beforeExecution(DurationExpression throttleDuration) {
        return BasicState.EXECUTABLE;
    }

    @Override
    public synchronized void afterSuccessfulExecution() {
    }

    @Override
    public synchronized Ack afterPositiveTriage() {
        return null;
    }

    @Override
    public synchronized void afterNegativeTriage() {

    }

    @Override
    public synchronized void ack(String user) {

    }

    @Override
    public synchronized boolean ackIfPossible(String user) {
        return false;
    }

}
