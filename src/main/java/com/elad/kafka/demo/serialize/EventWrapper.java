package com.elad.kafka.demo.serialize;

/**
 * User: eyalge
 * Date: 18/02/13
 * Time: 11:28
 */
public class EventWrapper {
    private Object event;

    public EventWrapper() {
    }

    public EventWrapper(Object event) {
        this.event = event;
    }

    public Object getEvent() {
        return event;
    }

    public void setEvent(Object event) {
        this.event = event;
    }
}
