package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.*;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {

	public Main nextServer;

	private static Cache cache;
	private static Map<String, Metadata> metadata;
	private String hash;
	public String start;
	public String end;

	public Main(Cache cache, Map<String, Metadata> metadata, String hash){
		if (cache.getClass().equals(FIFOLRUCache.class)){
			this.cache = (FIFOLRUCache) cache;
		} else if (cache.getClass().equals(LFUCache.class)){
			this.cache = (LFUCache) cache;
		}
		this.metadata = metadata;
		this.hash = hash;
		this.start = metadata.get(hash).getStart();
		this.end = metadata.get(hash).getEnd();
	}

	/**
	 * main() method where our serversocket will be initialized
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Config cfg = parseCommandlineArgs(args); // Do not change this
		setupLogging(cfg.logfile);
		KVStoreProcessor kvStore = new KVStoreProcessor();
		kvStore.setPath(cfg.dataDir);

		// now we can open a listening serversocket
		final ServerSocket serverSocket = new ServerSocket();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Closing thread per connection kv server");
//				shuttingDown = true;
				try {
//					if(shutDown)
						serverSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		// binding to the server
		serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

		KVCommandProcessor logic = new KVCommandProcessor(kvStore, cache);

		while (true) {
			// Waiting for client to connect
			Socket clientSocket = serverSocket.accept();

			// When we accept a connection, we start a new Thread for this connection
			Thread th = new ConnectionHandleThread(logic, clientSocket, metadata, cfg.bootstrap, cfg.listenaddr, cfg.port);
			th.start();
		}
	}


}
