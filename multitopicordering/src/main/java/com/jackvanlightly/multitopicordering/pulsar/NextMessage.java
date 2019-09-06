package com.jackvanlightly.multitopicordering.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

public class NextMessage {
    private Message message;
    private int index;

    public NextMessage(Message message, int index) {
        this.message = message;
        this.index = index;
    }

    public Message getMessage() {
        return message;
    }

    public int getIndex() {
        return index;
    }
}
