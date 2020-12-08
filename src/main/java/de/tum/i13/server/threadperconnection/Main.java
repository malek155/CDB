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
	private Map<String, Metadata> metadata;

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

	public void setMetadata(Map<String, Metadata> metadata){
		this.metadata = metadata;
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

		// for now, we'll make it more elegant later
		boolean shuttingDown = false;
		boolean shutDown = false;
		boolean transferred = false;

		// at first create a connection to ecs
		try(Socket socket = new Socket(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort())){
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			while(!shutDown){
				if(transferred){
					out.write("tranferred" + "\r\n");
					out.flush();
				}
				if(shuttingDown){
					out.write("mayishutdownplz" + "\r\n");
					out.flush();
					if(in.readLine().equals("yesyoumay")){
						shutDown = true;
					}
				}
			}
			in.close();
			out.close();
		}catch(IOException ie){
			ie.printStackTrace();
		}

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
