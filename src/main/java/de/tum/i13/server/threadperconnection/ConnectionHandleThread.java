package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.Cache;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreProcessor;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
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

	private static Map<String, Metadata> metadata;
	private String ip;
	private int port;
	private final InetSocketAddress bootstrap;
	private final String hash;
	private boolean shuttingDown;

	public ConnectionHandleThread(KVCommandProcessor commandProcessor,
								  Socket clientSocket,
								  Map<String, Metadata> metadata,
								  InetSocketAddress bootstrap,
								  String ip,
								  int port) throws NoSuchAlgorithmException {
		this.cp = commandProcessor;
		this.clientSocket = clientSocket;
		ConnectionHandleThread.metadata = metadata;
		this.bootstrap = bootstrap;
		this.ip = ip;
		this.port = port;
		this.hash = hashMD5(ip + port);
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
				while ((firstLine = in.readLine()) != null) {
					String res = cp.process(firstLine);
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
					outECS.write("MayIShutDownPlease " + this.hash + "\r\n");
					outECS.flush();
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

	private void transfer(String transferTo, String ours){
		String newIP = metadata.get(transferTo).getIP();
		int newPort = metadata.get(transferTo).getPort();

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

	private String hashMD5(String key) throws NoSuchAlgorithmException {
		byte[] msgToHash = key.getBytes();
		byte[] hashedMsg = MessageDigest.getInstance("MD5").digest(msgToHash);

		//get the result in hexadecimal
		String result = new String(Hex.encodeHex(hashedMsg));
		return result;
	}

}
