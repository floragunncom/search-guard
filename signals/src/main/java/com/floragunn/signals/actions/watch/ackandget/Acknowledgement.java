package com.floragunn.signals.actions.watch.ackandget;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.time.Instant;

public class Acknowledgement {
    private final Instant acknowledgeTime;
    private final String acknowledgeByUser;
    private final String actionId;

    static class AcknowledgementWriter implements Writeable.Writer<Acknowledgement> {

        @Override
        public void write(StreamOutput stream, Acknowledgement acknowledgement) throws IOException {
            stream.writeInstant(acknowledgement.getAcknowledgeTime());
            stream.writeString(acknowledgement.getAcknowledgeByUser());
            stream.writeString(acknowledgement.getActionId());
        }
    }

    static class AcknowledgementReader implements Writeable.Reader<Acknowledgement> {

        @Override
        public Acknowledgement read(StreamInput stream) throws IOException {
            return new Acknowledgement(stream.readInstant(), stream.readString(), stream.readString());
        }
    }

    public Acknowledgement(Instant acknowledgeTime, String acknowledgeByUser, String actionId) {
        this.acknowledgeTime = acknowledgeTime;
        this.acknowledgeByUser = acknowledgeByUser;
        this.actionId = actionId;
    }

    public Instant getAcknowledgeTime() {
        return acknowledgeTime;
    }

    public String getAcknowledgeByUser() {
        return acknowledgeByUser;
    }

    public String getActionId() {
        return actionId;
    }
}
