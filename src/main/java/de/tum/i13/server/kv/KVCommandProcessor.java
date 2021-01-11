package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Metadata;

import java.io.IOException;
import java.math.BigInteger;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
	private String replicaStart;
	private String end;
	private String hash;
	// static boolean variable for read only
	private boolean readOnly;
	// boolean variable to know if the server is initiated from the ECS
	// volatile keyword because this variable is expected to be changed from another
	// thread
	private volatile boolean initiated;

	public KVCommandProcessor(){
	}

	public static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

	// new constructor having the metadata instance and start end of the range
	public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache, String ip,
							  int port) throws NoSuchAlgorithmException {
		this.kvStore = kvStore;
		this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
		kvStore.setCache(this.cache);
		this.hash = this.hashMD5(ip + port);
		this.end = this.hash;
		this.initiated = false;
		this.readOnly = true;
		logger.info("New thread for server started, initializing");
	}

	// if we will use the cache here it should be static so that only one instance
	// is accessed by all the KVCommandProcessors
	/**
	 * process method that handles the requests
	 * @param command - command got from a client or another server
	 * @return answer after processing
	 */
	@Override
	public String process(String command) throws Exception {

		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");
		Map<String, Metadata> tempMap = new HashMap<>();


		String reply = command;

		if (input[0].equals("put") || input[0].equals("get") || input[0].equals("delete")) {
			this.start = metadata.get(hash).getStart();
			if (isInTheRange(this.hashMD5(input[1]), start, end)) {
				KVMessage msg;
				String response = "";

				try {
					// the return value will be a KVMessageProcessor here and the methods can only
					// be put or get or delete
					// I will change it as a return

					// put request
					// adding new read only functionality
					if (!initiated) {
						logger.info("Server is under initialization");
						response = "server_stopped";
					} else {
						if (input[0].equals("put") && readOnly) {
							logger.info("Server is under rebalancing, only getting keys is available");
							response = "server_write_lock";
						}
						if ((input[0].equals("put") || input[0].equals("delete")) && !readOnly) {
							if (input.length != 3 && input[0].equals("put")){
								logger.warning("not a suitable command for putting keys-values!");
								throw new IOException("Put Request needs a key and a value !");
							} else if (input.length != 2 && input[0].equals("delete")){
								logger.warning("not a suitable command for deleting keys-values!");
								throw new IOException("Delete Request needs only a key !");
							}
							msg = input[0].equals("put") ? this.kvStore.put(input[1], input[2], hashMD5(input[1]))
									: this.kvStore.put(input[1], null, "");
							logger.info("status:" + msg.getStatus().toString());
							if (msg.getStatus().equals(StatusType.PUT_ERROR)) {
								logger.info("Error occured by getting a value ");
								response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
							} else{
								logger.info("Put a new kv-pair");
								response = msg.getStatus().toString() + " " + msg.getKey();
							}
						} else if (input[0].equals("get")) {
							if (input.length != 2){
								logger.warning("not a suitable command for getting values!");
								throw new Exception("Get Request needs only a key !");
							}
							msg = this.kvStore.get(input[1]);
							if (msg.getStatus().equals(StatusType.GET_ERROR)) {
								logger.info("Error occured by getting a value ");
								response = msg.getStatus().toString() + " " + msg.getKey();
							} else {
								logger.info("Got a value");
								response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
							}
						}
					}
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				reply = response;
			} else {
				logger.info("Server is not responsible for a key");
				reply = "server_not_responsible";
			}
		} else if (input[0].equals("You'reGoodToGo")){
			this.initiated = true;
			readOnly = false;
			if(KVCommandProcessor.metadata != null){
				this.start = metadata.get(hash).getStart();
				this.end = metadata.get(hash).getEnd();
			}
			logger.info("Server is ready");
			this.readOnly = false;
		} else if (input[0].equals("replica1")) {



			// putting a new line in a replica line by line
			//

			logger.info("Updating replica1");
		} else if (input[0].equals("replica2")) {


			// putting a new line in a replica line by line
			logger.info("Updating replica2");
		} else if (input[0].equals("keyrange")) {
			reply = "keyrange_success " + KVCommandProcessor.metadata.keySet().stream()
						.map(key -> KVCommandProcessor.metadata.get(key).getStart() + ","
								+ key + ","
								+ KVCommandProcessor.metadata.get(key).getIP() + ":"
								+ KVCommandProcessor.metadata.get(key).getPort())
						.collect(Collectors.joining(";"));
			logger.info("Updating metadata on the client side, sending");
		} else if (input[0].equals("transferring")) {
			this.kvStore.put(input[1], input[2], input[3]);
			logger.info("Putting a new kv-pair, transferred from other servers");
		} else if (input[0].equals("metadata")) {
			String[] entry = command.split("=");
			hash = entry[0].split(" ")[1];
			String[] metadatanew = entry[1].split(" ");
			tempMap.put(hash, new Metadata(metadatanew[0], Integer.parseInt(metadatanew[1]), metadatanew[2], metadatanew[3]));

			if(KVCommandProcessor.metadata==null){
				this.initiated = true;
				this.readOnly = false;
			}
			KVCommandProcessor.metadata = tempMap;
			logger.info("Updated metadata from ECS");
		} else if (input[0].equals("keyrange_read")) {
			// new task

			logger.info("Updating keyranges for a client to read");
		}
		else{
			logger.info(String.valueOf(initiated));
			reply = "error: wrong command, please try again!";
			logger.warning("Wrong input from a client");
		}
		if (readOnly && reply.length() == 0){
			reply = "the server is read only at the moment and can not handle any put request please try later ";
			logger.info("Server is under rebalancing its storage right now, readonly");
		}
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
	private boolean isInTheRange(String key, String start, String end){
		boolean result = false;

		BigInteger intKey = new BigInteger(key, 16);
		BigInteger intStart = new BigInteger(start, 16);
		BigInteger intEnd = new BigInteger(end, 16);

		// where the start < end
		if (intStart.compareTo(intEnd) == -1) {
			if (intKey.compareTo(intStart) >= 0 && intKey.compareTo(intEnd) <= 0)
				result = true;
		} else {
			if (intKey.compareTo(intStart) >= 0 || intKey.compareTo(intEnd)<=0)
				result = true;
		}

		return result;
	}

	public KVStoreProcessor getKVStore() {
		return this.kvStore;
	}

	public Map<String, Metadata> getMetadata(){
		return KVCommandProcessor.metadata;
	}

	public String hashMD5(String key) throws NoSuchAlgorithmException {
		MessageDigest msg = MessageDigest.getInstance("MD5");
		byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));
		String myHash = new BigInteger(1, digested).toString(16);

		return myHash;
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