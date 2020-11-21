package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ConnectionHandleThread extends Thread {

	private CommandProcessor cp;
	private Socket clientSocket;
	private boolean isActive = false;

	public ConnectionHandleThread(CommandProcessor commandProcessor, Socket clientSocket) {
		this.cp = commandProcessor;
		this.clientSocket = clientSocket;
	}

	@Override
	public void run() {
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
			PrintWriter out = new PrintWriter(
					new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));
			// first we call the connection accepted method of the commandprocessor
			InetSocketAddress remote = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
			cp.connectionAccepted(new InetSocketAddress(clientSocket.getLocalPort()), remote);

			String firstLine;
			while ((firstLine = in.readLine()) != null) {
				// lehne bech takra el message eli jey mel client ou ta3malou l process
				String res = cp.process(firstLine);
				// tab3eth el resultat mte3 el process lil serveur
				out.write(res);
				out.flush();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			// handle the exception and add finally block to close everything
		}
	}
}
