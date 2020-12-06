package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.*;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {
	// used to shut down the server , maybe we need it
	private static boolean isRunning = true;
	private static Cache cache;
	private static KVStoreProcessor kvStore;
	public String start;
	public String end;
	private Metadata metadata;

	public Main nextServer;

	public Main(Cache cache, String start, String end){
		if (cache.getClass().equals(FIFOLRUCache.class)){
			cache = (FIFOLRUCache) cache;
		} else if (cache.getClass().equals(LFUCache.class)){
			cache = (LFUCache) cache;
		}
		this.start = start;
		this.end = end;
	}

	public void setMetadata(Metadata metadata){
		this.metadata = metadata;
	}

	/**
	 * Closing the server
	 */
	public void close(){
		isRunning = false;
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

		KVCommandProcessor logic = new KVCommandProcessor(kvStore, cache);

		while (isRunning) {
			// Waiting for client to connect
			Socket clientSocket = serverSocket.accept();

			// When we accept a connection, we start a new Thread for this connection
			Thread th = new ConnectionHandleThread(logic, clientSocket);
			th.start();
		}

	}
}
