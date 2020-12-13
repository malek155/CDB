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
	// used to shut down the server , maybe we need it
	private boolean isRunning = true;
	private static Cache cache;
	private KVStoreProcessor kvStore;
	public String start;
	public String end;
	private static Map<String, Metadata> metadata;
	private String nextIP;
	private int nextPort;
	private File storage;

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

		// first we connect to the ecs


		// now we can open a listening serversocket
		final ServerSocket serverSocket = new ServerSocket();


		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shuttingDown = true;
				try {
					if(shutDown)
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
			Thread th = new ConnectionHandleThread(logic, clientSocket);
			th.start();
		}
	}

	public void ecsConnect(String ip, int port){
		boolean notShutDown = true;
		boolean shuttingDown = false;

		try(Socket socket = new Socket(ip, port)){
			BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter outECS = new PrintWriter(socket.getOutputStream());
			while(notShutDown){
				if(shuttingDown){
					outECS.write("mayishutdownplz " + this.end + "\r\n");
					outECS.flush();
					if(inECS.readLine().equals("yesyoumay")){
						this.transfer("");
						outECS.write("merged" + "\r\n");
						outECS.flush();
						notShutDown = false;
					}
				}
//				if(inECS.readLine().equals("newServer")){
//					transfer(inECS.readLine());
//					outECS.write("transferred" + "\r\n");
//					outECS.flush();
//				}
			}
			inECS.close();
			outECS.close();
		}catch(IOException ie){
			ie.printStackTrace();
		}
	}

	public void transfer(String hash){
		nextIP = metadata.get(nextServer).getIP();
		nextPort = metadata.get(nextServer).getPort();
		try(Socket socket = new Socket(nextIP, nextPort)){
			storage = this.kvStore.getStorage(hash);
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
