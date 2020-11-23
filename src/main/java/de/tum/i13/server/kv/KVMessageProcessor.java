package de.tum.i13.server.kv;

public class KVMessageProcessor implements KVMessage {
    KVMessage kv;
    private StatusType status;
    private String key;
    private String value;

    public KVMessageProcessor(StatusType status, String key, String value) {
        this.key = key;
        this.status = status;
        this.value = value;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }
}