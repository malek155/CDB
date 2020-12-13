package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Metadata;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
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

	// static instance of metadata
	private static Map<String, Metadata> metadata;
	// start and end (for now I suppose that I am able to get them from the main)
	private String start;
	private String end;
	// static boolean variable for read only
	private static boolean readOnly;
	// boolean variable to know if the server is initiated from the ECS
	private boolean initiated = false;

	public KVCommandProcessor() {
	}

	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache) {
		this.kvStore = kvStore;
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
		kvStore.setCache(this.cache);
	}

	// new constructor having the metadata instance and start end of the range
	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache, Map<String, Metadata> metadata, String start,
			String end) {
		this.kvStore = kvStore;
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
		kvStore.setCache(this.cache);
		this.metadata = metadata;
		this.start = start;
		this.end = end;

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

		// Parse message "put message", call kvstore.put
		if (input[0].equals("put") || input[0].equals("get")) {
			if (isInTheRange(input[1], start, end)) {
				KVMessage msg;
				String response = "";

				try {
					// the return value will be a KVMessageProcessor here and the methods can only
					// be put or get or delete
					// I will change it as a return

					// put request
					// adding new read only functionality
					if (input[0].equals("put") && !readOnly) {
						if (input.length != 3) {
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
			} else {
				reply = "server_not_responsible";
			}
		} else if (input[0].equals("logLevel")) {
			logger.setLevel(Level.parse(input[1]));
			// here should be a msg !
			// why this transferring ??
		} else if (input[0].equals("transferring")) {

		} else {
			// here should be the send request because a wrong request will be handled in
			// the client side
			// logger.warning("Please check your input and try again.");
		}
		// if readOnly and reply =="" then reply should contain a msg telling the client
		// that he is readonly and can not handle put request
		if (readOnly && reply.length() == 0)
			reply = "the server is read only at the moment and can not handle any put request please try later ";
		return reply;
	}

	/**
	 * isInTheRange Method that takes the key sent from the client and verify
	 * whether this key in within the range of this KVServer
	 * 
	 * @param key   given from the client
	 * @param start start value of hash
	 * @param end   end value of hash
	 * @return a boolean saying if the KVServers range contains this key
	 */
	private boolean isInTheRange(String key, String start, String end) {
		boolean result = false;
		int intKey = (int) Long.parseLong(key, 16);
		int intStart = (int) Long.parseLong(start, 16);
		int intEnd = (int) Long.parseLong(end, 16);
		// where the start < end
		if (intStart < intEnd) {
			if (intKey >= intStart && intKey <= intEnd)
				result = true;
		} else {
			if (intKey >= intStart || intKey <= intEnd)
				result = true;
		}

		return result;
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