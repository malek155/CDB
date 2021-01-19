package de.tum.i13.server.ecs;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ECSConnection implements Runnable {
	private Socket clientSocket;
	private ECS bigECS; // is watching you
	private BufferedReader in;
	private PrintWriter out;
	private String ip;
	private int port;

	public static Logger logger = Logger.getLogger(ECSConnection.class.getName());

	public ECSConnection(Socket clientSocket, ECS bigECS) throws IOException {
		this.clientSocket = clientSocket;
		this.bigECS = bigECS;
		in = new BufferedReader(
				new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
		out = new PrintWriter(
				new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));
		ip = clientSocket.getInetAddress().getHostAddress();
		port = clientSocket.getLocalPort();
	}

	@Override
	public void run(){
		try {
			logger.info("Started the ECS connection");

			String line;
			while (!clientSocket.isClosed()){
				line = in.readLine();
				String message = this.process(line);
				Thread.yield();
				if (!message.equals("")) {
					out.write(message);
					out.flush();
				}
				if(bigECS.getMoved()){
					bigECS.movedMeta();
				}
				if (bigECS.isNewlyAdded() && bigECS.getServerRepository().size() > 1){
					bigECS.notifyServers();
					logger.info("Notifying a server, that it needs to send a data to a new server");
				}
			}
			logger.info("Closing the ECS connection");
			if (out != null)
				out.close();
			if (in != null) {
				in.close();
				clientSocket.close();
			}
		} catch (Exception ie) {
			ie.printStackTrace();
		}
	}

	private String process(String line) throws Exception {
		logger.info("processing");
		String reply = "";
		String[] ipport;
		String[] lines = line.split(" ");
		if (lines[0].equals("MayIShutDownPlease")) {
			ipport = lines[1].split(":");
			String nextHash = this.bigECS.shuttingDown(ipport[0], Integer.parseInt(ipport[1]), lines[2]);
			reply = "YesYouMay\r\n" + nextHash + "\r\n";
		}
		else if (lines[0].equals("IAmNew")) {
			ipport = lines[1].split(":");
			if(!bigECS.isAdded(ipport[0], Integer.parseInt(ipport[1]))){
				bigECS.addServer(ipport[0], Integer.parseInt(ipport[1]));
			}
		}
		return reply;
	}

	public void sendMeta(){
		Map<String, Metadata> map = bigECS.getMetadataMap();
		String metadata = map.keySet().stream()
				.map(key -> "metadata " + key + "=" + map.get(key).toString())
				.collect(Collectors.joining("\r\n"));
		out.write("first"+ metadata + " last" + "\r\n");
		out.flush();
	}

	public void reallocate(){
		out.write("NewServer\r\n" + bigECS.getNewServer() + "\r\n" + bigECS.getNextHash() + "\r\n");
		if(bigECS.getServerRepository().size()>2)
			out.write(bigECS.getNextNextHash() + "\r\n" + bigECS.getPrevHash() + "\r\n");
		else
			out.write(" \r\n \r\n");
		out.flush();
	}

	public String getIP(){return this.ip;}

	public int getPort(){return port;}

}