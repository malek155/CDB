package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.*;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

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
	private static Map<String, Metadata> metadata;
	private static boolean shuttingDown = false;
	private static boolean shutDown = false;
	private static String nextIP;
	private static int nextPort;
	private static File storage;

	// I don't agree on this instance
	public Main nextServer;

	public Main(Cache cache, String start, String end) {
		if (cache.getClass().equals(FIFOLRUCache.class)) {
			cache = (FIFOLRUCache) cache;
		} else if (cache.getClass().equals(LFUCache.class)) {
			cache = (LFUCache) cache;
		}
		this.start = start;
		this.end = end;
	}

	public void findNextIP() {
		this.nextIP = metadata.get(nextServer).getIP();
	}

	public void findNextPort() {
		this.nextPort = metadata.get(nextServer).getPort();
	}

	public void setStart(String newstart) {
		start = newstart;
	}

	public void setEnd(String newend) {
		end = newend;
	}

	public void setStorage() {
		storage = kvStore.getStorage();
	}

	public void setMetadata(Map<String, Metadata> metadata) {
		this.metadata = metadata;
	}

	public String getStart() {
		return this.start;
	}

	public String getEnd() {
		return this.end;
	}

	/**
	 * Closing the server
	 */
	public void close() {
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
		// now you can connect to ecs
		try (Socket socket = new Socket(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort())) {
			BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter outECS = new PrintWriter(socket.getOutputStream());
			while (!shutDown) {
				if (shuttingDown) {
					// But Having static start and end will cause that all the instances have the
					// same value of them !!
					outECS.write("mayishutdownplz " + end + "\r\n");
					outECS.flush();
					if (inECS.readLine().equals("yesyoumay")) {
						transfer();
						outECS.write("transferred" + "\r\n");
						outECS.flush();
						shutDown = true;
					}
				}
			}
			inECS.close();
			outECS.close();
		} catch (IOException ie) {
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
		// I can not add the begin and end because of the static reference !!
		// I am thinking about new object in main that contain the begin and end but
		// they will be changing every time so I have to check it with aiina !
		KVCommandProcessor logic = new KVCommandProcessor(kvStore, cache,metadata, start , end);

		while (isRunning) {
			// Waiting for client to connect
			Socket clientSocket = serverSocket.accept();

			// When we accept a connection, we start a new Thread for this connection
			Thread th = new ConnectionHandleThread(logic, clientSocket);
			th.start();
		}

	}

	public static void transfer() {
		try (Socket socket = new Socket(nextIP, nextPort)) {
			PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
			Scanner scanner = new Scanner(new FileInputStream(storage));

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				outTransfer.write("transferring " + scanner.nextLine() + "\r\n");
				outTransfer.flush();
			}
			scanner.close();
			outTransfer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
