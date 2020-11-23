package de.tum.i13.server.echo;

import de.tum.i13.server.kv.Cache;
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
	public EchoLogic(Cache cache, KVStore kvStore) {
		this.cache = cache;
	}

	public static Logger logger = Logger.getLogger(EchoLogic.class.getName());
	Cache cache;
	KVCommandProcessor CommProc = new KVCommandProcessor(new KVStoreProcessor(), this.cache);

	public String process(String command)  {

		logger.info("received command: " + command.trim());
		String[] input = command.split(" ");
		String response = "";
		if (input[0].equals("put") || input[0].equals("get") || input[0].equals("delete")) {
			// we have to make sure that the user uses minimum 2 components in the put
			// request otherwise we have to make an exception class for the put, get and
			// delete to
			// handle the unwanted requests but they should be thrown in the
			// KVCommandProcessor

			response = CommProc.process(command);// normally here we need the KVStore processor

//		} else if (input[0].equals("connect")) {
//			// Here we will use the connectionAccepted method but we don't have access to
//			// the remote and local socket , how should we implement it ?
//			// response = connectionAccepted() ;
//			// I think also we need an instance of active connection here to bind the client
//			// but we still don't have a clear client interface we have to integrate it
//		} else if (input[0].equals("disconnect")) {
			// same as connect matter
		} else if (input[0].equals("loglevel"))

		{
			//
		} else if (input[0].equals("help")) {

		} else if (input[0].equals("quit")) {

		} else {
			System.out.println("we have an exception here ");
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
