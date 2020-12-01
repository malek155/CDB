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

public class EchoLogic implements CommandProcessor {

	public EchoLogic(Cache cache, KVStore kvStore) {
		this.cache = cache;
	}

	public static Logger logger = Logger.getLogger(EchoLogic.class.getName());
	Cache cache;
	KVCommandProcessor CommProc = new KVCommandProcessor(new KVStoreProcessor(), this.cache);

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
