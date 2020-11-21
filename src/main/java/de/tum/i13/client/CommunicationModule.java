package de.tum.i13.client;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVStore;

public class CommunicationModule implements KVStore {

    @Override
    public KVMessage put(String key, String value) throws Exception {
        return null;
    }

    @Override
    public KVMessage get(String key) throws Exception {
        return null;
    }
}
