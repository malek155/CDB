package de.tum.i13.server.echo;

import de.tum.i13.server.kv.Cache;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVStoreProcessor;

import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Echologic class to interact with the client from the server perspective
 * 
 * @author gr9
 *
 */
public class EchoLogic implements CommandProcessor {

	KVStore kvStore;
	KVCommandProcessor CommProc;
	Cache cache;

	/**
	 * Constructor having two parameters
	 * 
	 * @param cache   the static cach
	 * @param kvStore interacting with the strorage
	 */
	public EchoLogic(Cache cache, KVStore kvStore) {
		this.cache = cache;
		this.kvStore = kvStore;
		this.CommProc = new KVCommandProcessor(new KVStoreProcessor(), this.cache);
	}

	public static Logger logger = Logger.getLogger(EchoLogic.class.getName());

	/**
	 * processes the commands given by user.
	 *
	 * @param command given the user.
	 * @return a string response with the actual status
	 * @throws Exception if put command cannot be executed (e.g. not connected to
	 *                   any KV server).
	 */
	public String process(String command) {

		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");

		String response = command;
		if (input[0].equals("put") || input[0].equals("get")) {

			response = CommProc.process(command);// normally here we need the KVStore processor

//		
		} else if (input[0].equals("logLevel")) {
			logger.setLevel(Level.parse(input[1]));
		}

		else {
			logger.warning("Please check your input and try again.");
		}
		// Let the magic happen here

		return response;
	}

	/**
	 * notifying the client that he is connected
	 */
	@Override

	public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
		logger.info("new connection: " + remoteAddress.toString());

		return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
	}

	/**
	 * notification of disconnection
	 */
	@Override
	public void connectionClosed(InetAddress remoteAddress) {
		logger.info("connection closed: " + remoteAddress.toString());

	}
}
