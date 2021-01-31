package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.*;
import de.tum.i13.shared.Config;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import static de.tum.i13.shared.Config.parseCommandlineArgs;

/**
 * Created by chris on 09.01.15.
 */
public class Main {

	public Main nextServer;
	private static Cache cache;
	public String start;
	public String end;
	public static ArrayList<ConnectionHandleThread> clientConnections = new ArrayList<>();
	public static ArrayList<String> updatedSubs;

	public Main(){}

	public void notifyClients(){
		for(String subscriber : updatedSubs){
			String[] subs = subscriber.split(" ");
			for(ConnectionHandleThread connection : clientConnections){
				if(subs[2].equals(connection.getClientSocket().getInetAddress().getHostAddress())
				 && Integer.parseInt(subs[3]) == connection.getClientSocket().getPort())
					connection.notifyClient();
			}
		}
	}

	/**
	 * main() method where our serversocket will be initialized
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {


		Config cfg = parseCommandlineArgs(args); // Do not change this
		KVStoreProcessor kvStore = new KVStoreProcessor(cfg.dataDir);

		if (cfg.cache.equals("FIFO")) {
			cache = new FIFOLRUCache(cfg.cacheSize, false);
		} else if (cfg.cache.equals("LRU")) {
			cache = new FIFOLRUCache(cfg.cacheSize, true);
		} else if (cfg.cache.equals("LFU")) {
			cache = new LFUCache(cfg.cacheSize);
		} else System.out.println("Please check your input for a cache strategy and try again.");

		kvStore.setCache(cache);

		// now we can open a listening serversocket
		final ServerSocket serverSocket = new ServerSocket();

		KVCommandProcessor logic = new KVCommandProcessor(kvStore, cache, cfg.listenaddr, cfg.port);
		InnerConnectionHandleThread innerThread = new InnerConnectionHandleThread(logic, cfg.bootstrap, cfg.listenaddr, cfg.port);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Closing thread per connection kv server");
				try {
					innerThread.setShuttingDown(true);
					while(!innerThread.getShutDown()){
						Thread.sleep(2000);
					}
					serverSocket.close();
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		// binding to the server
		serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

		new Thread(innerThread).start();

		while(true){
			// Waiting for client to connect
			Socket clientSocket = serverSocket.accept();

			// When we accept a connection, we start a new Thread for this connection
			ConnectionHandleThread clientThread = new ConnectionHandleThread(logic, clientSocket);

			// removing of a server in connection???? do it later
			clientConnections.add(clientThread);
			new Thread(clientThread).start();

			if(logic.getUpdateMainSids()) {
				Main.updatedSubs = logic.getSubscriptions();
				logic.setUpdateMainSids(false);
			}

		}
	}


}
