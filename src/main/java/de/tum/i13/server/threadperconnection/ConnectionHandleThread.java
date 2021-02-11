package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Logger;

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
	private boolean shuttingDown;
	private boolean closing;
	private int seconds;

	public ConnectionHandleThread(KVCommandProcessor commandProcessor,
								  Socket clientSocket, int seconds){
		this.cp = commandProcessor;
		this.clientSocket = clientSocket;
		this.shuttingDown = false;
		this.closing = false;
		this.seconds = seconds;
	}

	public static Logger logger = Logger.getLogger(ConnectionHandleThread.class.getName());

	@Override
	/*
	 * run() method
	 */
	public void run() {

		try {
			logger.info("Started a new client connection");

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

			String firstLine;
			String res;

			while (!clientSocket.isClosed()){
				while ((firstLine = in.readLine()) != null){
					if(firstLine.startsWith("subscribe")){
						res = cp.process(firstLine + " " + clientSocket.getInetAddress().getCanonicalHostName())  + "\r\n";
					}
					else
						res = cp.process(firstLine) + "\r\n";
					out.write(res);
					out.flush();
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex.getMessage());
		}

		cp.connectionClosed(remote.getAddress());

		try {
			shuttingDown = true;
			if(this.closing){
				logger.info("Closing a client connection");
				clientSocket.close();
				in.close();
				out.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Socket getClientSocket(){
		return this.clientSocket;
	}
}
