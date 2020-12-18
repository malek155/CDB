package de.tum.i13.server.ecs;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.stream.Collectors;

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

		while (!clientSocket.isClosed()) {
			try {
				in = new BufferedReader(
						new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
				out = new PrintWriter(
						new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

				String line;

				line = in.readLine();
				String message = this.process(line);
				if (!message.equals("")) {
					out.write(message);
					out.flush();
				}

				if (bigECS.moved) {
					Map<String, Metadata> map = bigECS.getMetadataMap();
					String metadata = map.keySet().stream()
							.map(key -> "metadata " + key + "=" + map.get(key).toString())
							.collect(Collectors.joining("\r\n"));
					out.write(metadata);
					out.flush();
					this.bigECS.setMoved(false);
				}
				if (bigECS.newlyAdded) {
					out.write("newserver\r\n" + bigECS.newServer + "\r\n" + bigECS.neighbourHash + "\r\n");
					out.flush();
					bigECS.newlyAdded = false;
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
	}

	private String process(String line) {
		String reply = "";
		String[] lines = line.split(" ");
		if (lines[0].equals("mayishutdownplz")) {
			String serverTransferTo = this.bigECS.shuttingDown(lines[1]);
			reply = "yesyoumay\r\n" + serverTransferTo + "\r\n";
		}
		return reply;
	}

}
