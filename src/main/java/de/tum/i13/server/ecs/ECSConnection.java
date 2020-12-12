package de.tum.i13.server.ecs;

import de.tum.i13.shared.Constants;

import java.io.*;
import java.net.Socket;

public class ECSConnection implements Runnable {
	private Socket clientSocket;
	private ECS bigECS; // is watching you

	public ECSConnection(Socket clientSocket, ECS bigECS) {
		this.clientSocket = clientSocket;
		this.bigECS = bigECS;
	}

	@Override
	public void run() {
		BufferedReader in = null;
		PrintWriter out = null;

		try {
			in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
			out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

			String line;
			while ((line = in.readLine()) != null) {
				String message = this.process(line);
				if (!message.equals("")) {
					out.write(message);
					out.flush();
				}
			}
		} catch (IOException ie) {
			ie.printStackTrace();
		} finally {
			try {
				if (out != null)
					out.close();
				if (in != null) {
					out.close();
					clientSocket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private String process(String line) {
		String reply = "";
		String[] lines = line.split(" ");
		if (lines[0].equals("mayishutdownplz")) {
			boolean may = this.bigECS.shuttingDown(lines[1]);
			reply = (may) ? "yesyoumay" : "";
		} else if (line.equals("transferred")) {
			this.bigECS.transferred(true);
		}
		return reply;
	}

}