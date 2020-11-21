package de.tum.i13.server.echo;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVStoreProcessor;
import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class EchoLogic implements CommandProcessor {
	// as we will use only one instance of Echologic ( main Class) then we will be
	// using only one instance of KVCommandProcessor and KVStore
	public static Logger logger = Logger.getLogger(EchoLogic.class.getName());
	KVCommandProcessor CommProc = new KVCommandProcessor(new KVStoreProcessor());

	public String process(String command) {

		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");
		switch (input[0]) {
		// we have to make sure that the user uses minimum 2 components in the put
		// request otherwise we have to make an exception class for the put and get to
		// handle the unwanted requests but they should be thrown in the
		// KVCommandProcessor
		case "put":
			CommProc.process(command);// normally here we need the KVStore processor
		case "get":
		}
		// Let the magic happen here

		return command;
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
