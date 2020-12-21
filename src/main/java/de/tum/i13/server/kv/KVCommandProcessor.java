package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage;
import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Metadata;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Hex;

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
	private KVStoreProcessor kvStore;
	private Cache cache;

	// static instance of metadata
	private static Map<String, Metadata> metadata;
	// start and end (for now I suppose that I am able to get them from the main)
	private String start;
	private String end;
	private String hash;
	// static boolean variable for read only
	private static boolean readOnly;
	// boolean variable to know if the server is initiated from the ECS
	// volatile keyword because this variable is expected to be changed from another
	// thread
	private volatile boolean initiated = false;

	public KVCommandProcessor() {
	}

	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache) {
		this.kvStore = kvStore;
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
		kvStore.setCache(this.cache);
	}

	// new constructor having the metadata instance and start end of the range
	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache, Map<String, Metadata> metadata, String ip,
			int port) throws NoSuchAlgorithmException {
		this.kvStore = kvStore;
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
		kvStore.setCache(this.cache);
		this.metadata = metadata;
		this.hash = this.hashMD5(ip + port);
		this.start = metadata.get(hash).getStart();
		this.end = metadata.get(hash).getEnd();
	}

	public static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

	// if we will use the cache here it should be static so that only one instance
	// is accessed by all the KVCommandProcessors
	/**
	 * process() method that handles the requests
	 */
	@Override
	public String process(String command) throws Exception {

		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");
		Map<String, Metadata> tempMap = new HashMap<>();
		;

		String reply = command;

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
					if (!initiated) {
						response = "server_stopped";
					} else {
						if (input[0].equals("put") && readOnly) {
							response = "server_write_lock";
						}
						if (input[0].equals("put") && !readOnly) {
							if (input.length < 4) {
								throw new IOException("Put Request needs a key and a value !");
							}
							msg = this.kvStore.put(input[1], input[2], input[3]);
							if (msg.getStatus().equals(StatusType.PUT_ERROR)) {
								response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
							} else {
								response = msg.getStatus().toString() + " " + msg.getKey();
							}
						} else if (input[0].equals("get")) {
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
		} else if (input[0].equals("keyrange")) {
			
			/*
			 * the server will send the metadata to the client
			 */
			// structuring the metadata as following : "keyrange_success <kr-from>, <kr-to>, <ip:port>; <kr-from>, <kr-to>, <ip:port>;..."

		} else if (input[0].equals("transferring")) {
			this.kvStore.put(input[1], input[2], input[3]);
		} else if (input[0].equals("metadata")) {
			String[] entry = command.split("=");
			hash = entry[0];
			String[] metadata = entry[1].split(" ");
			tempMap.put(hash, new Metadata(metadata[0], Integer.parseInt(metadata[1]), metadata[2], metadata[3]));
			this.metadata = tempMap;

		} else {
			// here should be the send request because a wrong request will be handled in
			// the client side
			// logger.warning("Please check your input and try again.");
		}
		if (readOnly && reply.length() == 0)
			reply = "the server is read only at the moment and can not handle any put request please try later ";
		return reply;
	}

	// ip port start end
	/**
	 * processMetadata method parses the command with metadata from ecs and updated
	 * global metadata
	 *
	 * @param command given .
	 */
	// public void processMetadata(String command) {
	// Map<String, Metadata> tempMap = new HashMap<>();
	// String[] input = command.split("\r\n");
	// String[] entry;
	// String hash;
	// String[] metadata;
	//
	// for (int i = 0; i < input.length; i++) {
	// entry = input[i].split("=");
	// hash = entry[0];
	// metadata = entry[1].split(" ");
	// tempMap.put(hash, new Metadata(metadata[0], Integer.parseInt(metadata[1]),
	// metadata[2], metadata[3]));
	// }
	// metadataMap = tempMap;
	// }

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

	public KVStoreProcessor getKVStore() {
		return this.kvStore;
	}

	private String hashMD5(String key) throws NoSuchAlgorithmException {
		byte[] msgToHash = key.getBytes();
		byte[] hashedMsg = MessageDigest.getInstance("MD5").digest(msgToHash);

		// get the result in hexadecimal
		String result = new String(Hex.encodeHex(hashedMsg));
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