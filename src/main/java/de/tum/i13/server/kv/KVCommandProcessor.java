package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
    private KVStore kvStore;
    private Cache cache;

    public void setCacheStrategy(Cache cache){
        if (cache.getClass().equals(LFUCache.class))
            this.cache = (LFUCache) cache;
        else this.cache = (FIFOLRUCache) cache;
    }

    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    public String process(String command) {
        int keyDelimiter = 0;
        if (command.substring(0, 3).toLowerCase() == "put")
            keyDelimiter = command.substring(4).indexOf(' ');
        try {
            this.kvStore.put(command.substring(4, keyDelimiter), command.substring(keyDelimiter + 1));
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (command.substring(0, 3).toLowerCase() == "get") {
            keyDelimiter = command.substring(4).indexOf(' ');
            if (cache.get(command.substring(4))==null) {
                try {
                    this.kvStore.get(command.substring(4));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else{

            }
        } else {


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