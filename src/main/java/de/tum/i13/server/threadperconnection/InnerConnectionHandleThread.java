package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.KVCommandProcessor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * ConnectionHandleThread class that will handle the thread of each client
 *
 * @author gr9
 *
 */
public class InnerConnectionHandleThread extends Thread {

	private final KVCommandProcessor cp;

	private final InetSocketAddress bootstrap;
	private final String hash;
	private final String ip;
	private final int port;
	private final ConnectionHandleThread client;

	public InnerConnectionHandleThread(KVCommandProcessor commandProcessor, InetSocketAddress bootstrap, String ip,
			int port, ConnectionHandleThread client) throws NoSuchAlgorithmException {
		this.cp = commandProcessor;
		this.bootstrap = bootstrap;
		this.hash = hashMD5(ip + port);
		this.ip = ip;
		this.port = port;
		this.client = client;
	}

	public static Logger logger = Logger.getLogger(InnerConnectionHandleThread.class.getName());

	@Override
	/*
	 * run() method
	 */
	public void run() {

		boolean notShutDown = true;
		String nextNeighbour;
		String nextNextNeighbour;
		String prevNeighbour;
		String cutter;

		// ip, port -> bootstrap, ecs ip, port
		try (Socket socket = new Socket(bootstrap.getHostString(), bootstrap.getPort())) {
			BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter outECS = new PrintWriter(socket.getOutputStream());

			logger.info("Started an ECS connection");

			while (notShutDown) {
				if (inECS.readLine().equals("NewServer")) {
					cutter = inECS.readLine(); // newly added server
					nextNeighbour = inECS.readLine(); // server we have our data at
					nextNextNeighbour = inECS.readLine();
					prevNeighbour = inECS.readLine();

					if (nextNextNeighbour.equals(this.hash)) {
						this.cp.getKVStore().removeReplica2();
					}
					if (nextNeighbour.equals(this.hash)) {
						this.cp.getKVStore().removeReplica2();

						// in kvstoreprocessor toReturn should be saved to the fst replica
						this.transfer(cutter, nextNeighbour);
						logger.info("Transferred data to a new server");

						this.transferToNextNext(nextNextNeighbour);
						logger.info("Transferred replica of a new server to a next after next server");
					}
					if (prevNeighbour.equals(this.hash)) {
						this.transferFromPrev(cutter);
						logger.info("Transferred replicas to a new server");
					}
				}
				if (client.getShuttingDown()) {
					outECS.write("MayIShutDownPlease " + this.ip + ":" + this.port + " " + this.hash + "\r\n");
					outECS.flush();
					logger.info("Request to ECS to be allowed to shut down");

					Thread.yield();
					if (inECS.readLine().equals("YesYouMay")) {
						nextNeighbour = inECS.readLine();
						this.transfer(nextNeighbour, "");

						this.client.setClosing(true);
						notShutDown = false;
						logger.info("Shutting down in a process");
					}
				}
			}
			inECS.close();
			outECS.close();
			logger.info("Closed an ECS connection");
		} catch (IOException ie) {
			ie.printStackTrace();
		}
	}

	/**
	 * transfer method connects with a neighbour server to transfer all data if it
	 * is shutting down, otherwise only the part of a kvstorage to a new server
	 * 
	 * @param newServer  server transfer to
	 * @param nextServer is our server to transfer from, a neigbour
	 */
	private void transfer(String newServer, String nextServer) throws IOException {
		String newIP = cp.getMetadata().get(newServer).getIP();
		int newPort = cp.getMetadata().get(newServer).getPort();

		File storage = (nextServer.equals("")) ? this.cp.getKVStore().getStorage("")
				: this.cp.getKVStore().getStorage(newServer);

		try (Socket socket = new Socket(newIP, newPort)) {
			PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
			Scanner scanner = new Scanner(new FileInputStream(storage));

			while (scanner.hasNextLine()) {
				outTransfer.write("transferring " + scanner.nextLine() + "\r\n");
				outTransfer.flush();
			}
			if (nextServer.equals("")) {
				outTransfer.write("You'reGoodToGo\r\n");
				outTransfer.flush();
			}

			scanner.close();
			outTransfer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void transferFromPrev(String newServer) throws IOException {
		String newIP = cp.getMetadata().get(newServer).getIP();
		int newPort = cp.getMetadata().get(newServer).getPort();

		File replica1 = this.cp.getKVStore().getStorage("");
		File replica2 = this.cp.getKVStore().getReplica1();

		try (Socket socket = new Socket(newIP, newPort)) {
			PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
			Scanner scanner1 = new Scanner(new FileInputStream(replica1));
			Scanner scanner2 = new Scanner(new FileInputStream(replica2));

			while (scanner1.hasNextLine()) {
				outTransfer.write("replica1 " + scanner1.nextLine() + "\r\n");
				outTransfer.flush();
			}
			while (scanner2.hasNextLine()) {
				outTransfer.write("replica2 " + scanner1.nextLine() + "\r\n");
				outTransfer.flush();
			}

			scanner1.close();
			scanner2.close();
			outTransfer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void transferToNextNext(String nextNextServer) {
		File replica2 = this.cp.getKVStore().getReplica1();
		String nextNextIP = cp.getMetadata().get(nextNextServer).getIP();
		int nextNextPort = cp.getMetadata().get(nextNextServer).getPort();

		try (Socket socket = new Socket(nextNextIP, nextNextPort)) {
			PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
			Scanner scanner = new Scanner(new FileInputStream(replica2));

			while (scanner.hasNextLine()) {
				outTransfer.write("replica2 " + scanner.nextLine() + "\r\n");
				outTransfer.flush();
			}
			scanner.close();
			outTransfer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * hashMD5 method hashes a given key to its Hexadecimal value with md5
	 *
	 * @return String of hashvalue in Hexadecimal
	 */
	private String hashMD5(String key) throws NoSuchAlgorithmException {

		MessageDigest msg = MessageDigest.getInstance("MD5");
		byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));

		return new String(digested);
	}

}