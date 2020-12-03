package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * KVCommandProcessor to handle the client requests that contains put or get
 * requests
 * 
 * @author gr9
 *
 */
public class KVCommandProcessor implements CommandProcessor {
	// we forward the lines that have put , get , delete from the Echologic to this
	// class because it is responsible to interact with the KVStore and handle those
	// commands
	private KVStore kvStore;
	private Cache cache;

	public KVCommandProcessor(){
	}

	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache) {
		this.kvStore = kvStore;
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
		kvStore.setCache(this.cache);
	}

	public static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

	// if we will use the cache here it should be static so that only one instance
	// is accessed by all the KVCommandProcessors
	/**
	 * process() method that handles the requests
	 */
	@Override
	public String process(String command) {

		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");

		String reply = command;

		// TODO
		// Parse message "put message", call kvstore.put
		if (input[0].equals("put") || input[0].equals("get")) {
			KVMessage msg;
			String response = "";
			try {
				// the return value will be a KVMessageProcessor here and the methods can only
				// be put or get or delete
				// I will change it as a return

				// put request
				if (input[0].equals("put")) {
					if (input.length < 3) {
						throw new IOException("Put Request needs a key and a value !");
					}
					msg = this.kvStore.put(input[1], input[2]);
					if (msg.getStatus().equals(StatusType.PUT_ERROR)) {
						response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
					} else {
						response = msg.getStatus().toString() + " " + msg.getKey();
					}

				}
				// get request
				else if (input[0].equals("get")) {
					if (input.length != 2) {
						throw new Exception("Get Request needs only a key !");
					}
					msg = this.kvStore.get(input[1]);
					if (msg.getStatus().equals(StatusType.GET_ERROR)) {
						response = msg.getStatus().toString() + " " + msg.getKey();
					} else {
						response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			reply = response;
		} else if (input[0].equals("logLevel")) {
			logger.setLevel(Level.parse(input[1]));
			// here should be a msg !
		} else {
			// here should be the send request because a wrong request will be handled in
			// the client side
			// logger.warning("Please check your input and try again.");
		}
		return reply;
	}

	@Override
	public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
		logger.info("new connection: " + remoteAddress.toString());

		return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
	}

	@Override
	public void connectionClosed(InetAddress address) {
		logger.info("connection closed: " + address.toString());

	}
}
