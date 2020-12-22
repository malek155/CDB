package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Scanner;

/**
 * ConnectionHandleThread class that will handle the thread of each client
 * 
 * @author gr9
 *
 */
public class ConnectionHandleThread extends Thread {

	private final KVCommandProcessor cp;
	private final Socket clientSocket;

	private BufferedReader in = null;
	private PrintWriter out = null;
	private InetSocketAddress remote = null;

	private final InetSocketAddress bootstrap;
	private final String hash;
	private boolean shuttingDown;
	private String ip;
	private int port;

	public ConnectionHandleThread(KVCommandProcessor commandProcessor,
								  Socket clientSocket,
								  InetSocketAddress bootstrap,
								  String ip,
								  int port) throws NoSuchAlgorithmException {
		this.cp = commandProcessor;
		this.clientSocket = clientSocket;
		this.bootstrap = bootstrap;
		this.hash = hashMD5(ip + port);
		this.ip = ip;
		this.port = port;
	}

	@Override
	/*
	 * run() method
	 */
	public void run() {

		// run ecs connection
		ecsConnect(bootstrap.getHostString(), bootstrap.getPort());

		// run client connection
		clientConnect();
	}


	/**
	 * clientConnect method connects with ecs and communicates with it
	 */
	private void clientConnect(){
		boolean done = true;
		while (!clientSocket.isClosed()) {
			try {
				if (done) {
					in = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
					out = new PrintWriter(
							new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));
					// first we call the connection accepted method of the commandprocessor
					remote = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
					// So that we are sending the connectionaccepted msg only once in the beginning

					String firstMsg = cp.connectionAccepted(new InetSocketAddress(clientSocket.getLocalPort()), remote);
					out.write(firstMsg);
					out.flush();
					done = false;
				}
				String firstLine;
				String res;
				while ((firstLine = in.readLine()) != null) {
					res = cp.process(firstLine);

					out.write(res);
					out.flush();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				// handle the exception and add finally block to close everything
			}
		}
		// We display the disconnection notification
		// we maybe have to add sysout in the connectionClosed method in echoLogic
		cp.connectionClosed(remote.getAddress());
		// I will close anything here
		shuttingDown = true;

		try {
			in.close();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void ecsConnect(String ip, int port){
		boolean notShutDown = true;
		shuttingDown = false;
		String neighbour;
		String cutter;

		// ip, port -> bootstrap, ecs ip, port
		try(Socket socket = new Socket(ip, port)){
			BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter outECS = new PrintWriter(socket.getOutputStream());

			while(notShutDown){
				if(inECS.readLine().equals("NewServer")){
					cutter = inECS.readLine();  				// newly added server
					neighbour = inECS.readLine();			// server we have our data at
					if(neighbour.equals(this.hash)) {
						this.transfer(cutter, neighbour);
					}
				}
				if(shuttingDown){
					outECS.write("MayIShutDownPlease " + this.ip + ":" + this.port + " " + this.hash + "\r\n");
					outECS.flush();

					Thread.yield();
					if(inECS.readLine().equals("YesYouMay")){
						neighbour = inECS.readLine();
						this.transfer(neighbour, "");
						notShutDown = false;
					}
				}
			}
			inECS.close();
			outECS.close();
		}catch(IOException ie){
			ie.printStackTrace();
		}
	}


	/**
	 * transfer method connects with a neighbour server to transfer all data if it is shutting down,
	 * 	otherwise only the part of a kvstorage to a new server
	 * @param transferTo server transfer to
	 * @param ours is our server to transfer from, a neigbour
	 */
	private void transfer(String transferTo, String ours){
		String newIP = cp.getMetadata().get(transferTo).getIP();
		int newPort = cp.getMetadata().get(transferTo).getPort();

		try (Socket socket = new Socket(newIP, newPort)){
			File storage = (ours.equals("")) ? this.cp.getKVStore().getStorage("")
					: this.cp.getKVStore().getStorage(transferTo);
			PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
			Scanner scanner = new Scanner(new FileInputStream(storage));

			while (scanner.hasNextLine()) {
				outTransfer.write("transferring " + scanner.nextLine() + "\r\n");
				outTransfer.flush();
			}
			if(ours.equals("")){
				outTransfer.write("You'reGoodToGo" + "\r\n");
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
