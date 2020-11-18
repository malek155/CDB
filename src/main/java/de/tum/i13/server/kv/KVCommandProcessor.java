package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
    private KVStore kvStore;

    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public String process(String command) {
        //sarra parsing the possible commands
        //TODO
        if (command.toLowerCase() == "put")
            //Parse message "put message", call kvstore.put
            try {
                this.kvStore.put("key", "value");
            } catch (Exception e) {
                e.printStackTrace();
            }
        else if (command.toLowerCase() == "get") {
            try {
                this.kvStore.get("key");
            } catch (Exception e) {
                e.printStackTrace();
            }
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
