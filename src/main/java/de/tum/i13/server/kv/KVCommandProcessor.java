package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class KVCommandProcessor implements CommandProcessor {
	// we forward the lines that have put , get , delete from the Echologic to this
	// class because it is responsible to interact with the KVStore and handle those
	// commands
	private KVStore kvStore;
	private Cache cache;

	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache) {
		this.kvStore = kvStore;
		this.cache = cache;
		kvStore.setCache(cache);
	}

	// if we will use the cache here it should be static so that only one instance
	// is accessed by all the KVCommandProcessors
	@Override
	public String process(String command) {
		// TODO
		// Parse message "put message", call kvstore.put
		KVMessage msg;
		String response = "";
		try {
			// the return value will be a KVMessageProcessor here and the methods can only
			// be put or get or delete
			// I will change it as a return
			String[] array = command.split(" ");
			// put request
			if (array[0].equals("put")) {
				if (array.length < 3) {
					throw new IOException("Put Request needs a key and a value !");
				}
				msg = this.kvStore.put(array[1], array[2]);
				if (msg.getStatus().equals(StatusType.PUT_ERROR)) {
					response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
				} else {
					response = msg.getStatus().toString() + " " + msg.getKey();
				}

			}
			// get request
			else if (array[0].equals("get")) {
				if (array.length != 2) {
					throw new Exception("Get Request needs only a key !");
				}
				msg = this.kvStore.get(array[1]);
				if (msg.getStatus().equals(StatusType.GET_ERROR)) {
					response = msg.getStatus().toString() + " " + msg.getKey();
				} else {
					response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return response;
	}

	@Override
	public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
		// TODO

		return null;
	}

	@Override
	public void connectionClosed(InetAddress address) {
		// TODO

	}
}
