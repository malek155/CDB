package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
    private KVStore kvStore;
    private Cache cache;

    public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache) {
        this.kvStore = kvStore;
        this.cache = (cache.getClass().equals(LFUCache.class))?(LFUCache) cache:(FIFOLRUCache) cache;
    }

    @Override
    public String process(String command) {
        // TODO
        // Parse message "put message", call kvstore.put
        try {
            // the return value will be a KVMessageProcessor here and the methods can only
            // be put or get or delete
            // I will change it as a return
            KVMessage msg;
            String response;
            String[] array = command.split(" ");
            // put request
            if (array[0].equals("put")) {
                if (array.length < 3) {
                    throw new IOException("Wrong \"put\" command");
                }
                msg = this.kvStore.put(array[1], array[2]);
                if (msg.getStatus().equals(KVMessage.StatusType.PUT_ERROR)) {
                    response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
                } else {
                    response = msg.getStatus().toString() + " " + msg.getKey();
                }

            }
            // get request
            else if (array[0].equals("get")) {
                if (array.length != 2) {
                    throw new IOException("Wrong \"get\" command");
                }
                msg = this.kvStore.get(array[1]);
                if (msg.getStatus().equals(KVMessage.StatusType.GET_ERROR)) {
                    response = msg.getStatus().toString() + " " + msg.getKey();
                } else {
                    response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
                }

            }
            // this.kvStore.put("key", "hello");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        //TODO

        return null;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        //TODO

    }
}
