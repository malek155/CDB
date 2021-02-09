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
					if(firstLine.startsWith("subscribe") || firstLine.startsWith("unsubscribe")){
						res = cp.process(firstLine + " " + clientSocket.getInetAddress().getHostAddress() + "\r\n");
					}
					else
						res = cp.process(firstLine) + "\r\n";

					logger.info(res);
					out.write(res);
					out.flush();
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex.getMessage());
			// handle the exception and add finally block to close everything
		}

		// We display the disconnection notification
		// we maybe have to add sysout in the connectionClosed method in echoLogic
		cp.connectionClosed(remote.getAddress());
		// I will close anything here

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

	public void notifyClient(String key, String value) throws IOException, InterruptedException {
		if(out !=  null && in != null){
			int count = 0;
			String messageToSend = "notify " + key + " " + value + "\r\n";
			out.write(messageToSend);
			out.flush();
// to be continued
		}
	}

	public Socket getClientSocket(){
		return this.clientSocket;
	}
}
