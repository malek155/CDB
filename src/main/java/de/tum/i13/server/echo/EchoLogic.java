package de.tum.i13.server.echo;

import de.tum.i13.server.kv.*;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EchoLogic implements CommandProcessor {
	// as we will use only one instance of Echologic ( main Class) then we will be
	// using only one instance of KVCommandProcessor and KVStore

	private Cache cache;
	private KVStoreProcessor kvStore;
	public static Logger logger = Logger.getLogger(EchoLogic.class.getName());

	public EchoLogic(Cache cache, KVStoreProcessor kvStore) {
		this.cache = cache;
		if (cache.getClass().equals(FIFOLRUCache.class)) {
			this.cache = (FIFOLRUCache) this.cache;
		} else if (cache.getClass().equals(FIFOLRUCache.class)) {
			this.cache = (LFUCache) this.cache;
		}
		this.kvStore = kvStore;
	}

	KVCommandProcessor CommProc = new KVCommandProcessor(kvStore, cache);

	public String process(String command) {
		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");
		String response = "";
		if (input[0].equals("put") || input[0].equals("get")) {
			// we have to make sure that the user uses minimum 2 components in the put
			// request otherwise we have to make an exception class for the put, get and
			// delete to
			// handle the unwanted requests but they should be thrown in the
			// KVCommandProcessor
			response = CommProc.process(command);
		} else if (input[0].equals("loglevel")) {
			logger.setLevel(Level.parse(input[1]));
		} else {
			logger.warning("Please check your input and try again.");
		}
		return response;
	}

	@Override
	public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
		logger.info("new connection: " + remoteAddress.toString());
		return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
	}

	@Override
	public void connectionClosed(InetAddress remoteAddress) {
		logger.info("connection closed: " + remoteAddress.toString());
	}
}
