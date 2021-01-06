package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.*;
import de.tum.i13.shared.Config;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import static de.tum.i13.shared.Config.parseCommandlineArgs;

/**
 * Created by chris on 09.01.15.
 */
public class Main {

	public Main nextServer;
	public Main nextNextServer;
	private static Cache cache;
	public String start;
	public String end;

	public Main() {
		nextNextServer = nextServer.nextServer;
	}

	/**
	 * main() method where our serversocket will be initialized
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {

		Config cfg = parseCommandlineArgs(args); // Do not change this
		KVStoreProcessor kvStore = new KVStoreProcessor();
		kvStore.setPath(cfg.dataDir);

		// now we can open a listening serversocket
		final ServerSocket serverSocket = new ServerSocket();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Closing thread per connection kv server");
				try {
					serverSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		// binding to the server
		serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

		if (cfg.cache.equals("FIFO")) {
			cache = new FIFOLRUCache(cfg.cacheSize, false);
		} else if (cfg.cache.equals("LRU")) {
			cache = new FIFOLRUCache(cfg.cacheSize, true);
		} else if (cfg.cache.equals("LFU")) {
			cache = new LFUCache(cfg.cacheSize);
		} else
			System.out.println("Please check your input for a cache strategy and try again.");

		KVCommandProcessor logic = new KVCommandProcessor(kvStore, cache, cfg.listenaddr, cfg.port);

		while (true) {
			// Waiting for client to connect
			Socket clientSocket = serverSocket.accept();

			// When we accept a connection, we start a new Thread for this connection
			ConnectionHandleThread clientThread = new ConnectionHandleThread(logic, clientSocket);
			InnerConnectionHandleThread innerThread = new InnerConnectionHandleThread(logic, cfg.bootstrap,
					cfg.listenaddr, cfg.port, clientThread);

			new Thread(innerThread).start();
			new Thread(clientThread).start();
		}
	}

}