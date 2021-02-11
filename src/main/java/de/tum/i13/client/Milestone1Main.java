package de.tum.i13.client;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.*;
import java.util.stream.Stream;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

/**
 * Milestone1Main class that handles the client interaction
 *
 * @author gr9
 */
public class Milestone1Main {
	// the logger
	public static Logger logger = Logger.getLogger(Milestone1Main.class.getName());


	/**
	 * hashMD5 method hashes a given key to its Hexadecimal value with md5
	 *
	 * @return String of hashvalue in Hexadecimal
	 */
	public static String hashMD5(String key) throws NoSuchAlgorithmException {

		MessageDigest msg = MessageDigest.getInstance("MD5");
		byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));
		// return new String(digested);
		String myHash = new BigInteger(1, digested).toString(16);

		return myHash;

	}

	/*
	 * SEVERE WARNING INFO CONFIG FINE FINER FINEST
	 */

	public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException {
		ArrayList<Integer> listening = new ArrayList<Integer>();
		LogManager.getLogManager().reset();
		logger.setLevel(Level.ALL);
		// Console Handler
		ConsoleHandler ch = new ConsoleHandler();
		ch.setLevel(Level.WARNING);
		// adding the handler
		logger.addHandler(ch);
		// File Handler
		try {

			FileHandler fh = new FileHandler("client.log");
			// adding the handler
			logger.addHandler(fh);
		} catch (Exception e) {
			logger.severe("File logger not working ! ");
		}

		Map<String, Metadata> metadataMap = new HashMap<>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		ActiveConnection activeConnection = null;

		for (; ; ) {
			System.out.print("EchoClient> ");
			String line = reader.readLine();
			String[] command = line.split(" ");

			switch (command[0]) {
				case "connect":
					activeConnection = buildconnection(command);
					logger.info("Connecting to a server ");
					break;
				case "send":
					logger.info("a send request  ");
					sendmessage(activeConnection, command, line);
					break;
				case "put":
				case "get":
				case "delete":
				case "publish":
				case "subscribe":
				case "unsubscribe":
				case "keyrange":
				case "keyrange_read":
					logger.info("a put/get/delete request  ");
					// number of retry
					int count = 0;
					// the maximum number of retry is 5
					while (count <= 5) {
						// we check whether the metadata is empty
						if (!metadataMap.isEmpty() && !(command[0].equals("keyrange") || command[0].equals("keyrange_read"))) {
							logger.info("metadata is not empty");
							Metadata meta = null;
							try {
								// getting the server which is responsible of this key
								meta = getServer(metadataMap, hashMD5(command[1]));
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}
							String[] a = {null, meta.getIP(), "" + meta.getPort()};
							// building a new connection to this server
							activeConnection = buildconnection(a);

						}
						if (command[0].equals("subscribe")) {
							if (listening.isEmpty() || !listening.contains(Integer.parseInt(command[2]))) {
								listening.add(Integer.parseInt(command[2]));
								ServerSocket serverSocket = new ServerSocket();
								// there is no config for client side so I can not specify an ip for the client so I have no choice except listening in the local host !!
								serverSocket.bind(new InetSocketAddress(InetAddress.getLocalHost(), Integer.parseInt(command[3])));
								// waiting for the broker to connect
								Socket brokerSocket = serverSocket.accept();
								// we start a new thread for the broker
								Thread th = new Thread(new Runnable() {

									@Override
									public void run() {
										try {
											logger.info("Starting the ClientConnection to listen to the broker !");
											BufferedReader in = new BufferedReader(
													new InputStreamReader(brokerSocket.getInputStream(), Constants.TELNET_ENCODING));
											PrintWriter out = new PrintWriter(
													new OutputStreamWriter(brokerSocket.getOutputStream(), Constants.TELNET_ENCODING));
											String line;
											while (!brokerSocket.isClosed()) {
												line = in.readLine();
												String[] msg = line.split(" ");
												// the acknowledgement
												if (msg[0].equals("notify")) {
													String ack = "I got it !";
													Thread.yield();

													out.write(ack);
													out.flush();
												}


											}


										} catch (IOException e) {
											e.printStackTrace();
										}
									}
								});
							}
						}
						// send the request
						String result = sendrequest(activeConnection, command, line);
						// if we get a server_write lock or server_stopped notification from the server
						// we wait for some time and we retry
						if (result.equals("not a suitable command for putting keys-values!") || result.equals("Error! Not connected!")) {
							logger.info(result);
							break;
						} else if (result.substring(0, 9).equals("GET_ERROR") ||
								result.substring(0, 9).equals("PUT_ERROR") ||
								result.substring(0, 10).equals("PUT_UPDATE") ||
								result.substring(0, 11).equals("PUT_SUCCESS") ||
								result.substring(0, 11).equals("GET_SUCCESS") ||
								result.substring(0, 12).equals("DELETE_ERROR") ||
								result.substring(0, 14).equals("DELETE_SUCCESS") ||
								result.substring(0, 16).equals("keyrange_success") ||
								result.substring(0, 17).equals("PUBLICATION_ERROR") ||
								result.substring(0, 17).equals("SUBSCRIBE_SUCCESS") ||
								result.substring(0, 19).equals("PUBLICATION_SUCCESS") ||
								result.substring(0, 19).equals("UNSUBSCRIBE_SUCCESS") ||
								result.substring(0, 21).equals("keyrange_read_success")) {
							logger.info(result);
							break;
						}
						if (result.equals("server_write_lock") || result.equals("server_stopped")) {
							logger.info("the server is in write lock , or the server is stopped ");
							count++;

							// exponential back-off with jitter
							int base = 100;
							int cap = 5000;
							int a = (int) Math.pow(2, count);
							int temp = Math.min(cap, base * a);
							int random = (int) (Math.random() * ((temp / 2) - 0) + 0);
							Thread.sleep((temp / 2) + random);

						} // if we get a server_not_responsible notification from the server that means
						// that our metadata is stale and we need to update it
						else if (result.equals("server_not_responsible")) {
							// we ask the server to send us the most recent metadata version
							logger.info(
									"the server is not responsible for this range , we send a keyrange request to update the metadata");
							activeConnection.write("keyrange" + "\r\n");
							Thread.yield();
							/*
							 * reading the metadata from the server and updating its metadata
							 */
							String metadata = activeConnection.readline();
							String[] entry = metadata.split(";");
							Stream<String> sp = Arrays.stream(entry);
							Map<String, Metadata> metadataMap2 = new HashMap<>();
							sp.forEach(str -> {
								String[] entry2 = str.split(",");
								if (entry2[0].substring(0, 17).equals("keyrange_success "))
									entry2[0] = entry2[0].substring(17);
								String[] ipAndPort = entry2[2].split(":");
								metadataMap2.put(entry2[1],
										new Metadata(ipAndPort[0], Integer.parseInt(ipAndPort[1]), entry2[0], entry2[1]));

							});
							metadataMap = metadataMap2;
							logger.info("updated metadata !");
							Metadata meta = null;
							try {
								// getting the server which is responsible of this key
								meta = getServer(metadataMap, hashMD5(command[1]));
							} catch (NoSuchAlgorithmException e) {
								logger.warning("Error in getting the right server from the received metadata");
								e.printStackTrace();
							}
							String[] a = {null, meta.getIP(), "" + meta.getPort()};
							// building a new connection to this server

							activeConnection = buildconnection(a);
							// retry the request to the new server
							sendrequest(activeConnection, command, line);
							logger.info("building of new connection and resending of the previous command ");
							// updating count
							count = 0;

						}
					}

					break;

				case "disconnect":
					logger.info("the client is disconnecting");
					closeConnection(activeConnection);
					break;
				case "help":
					logger.info("help request");
					printHelp();
					break;
				case "quit":
					logger.info("quit request");
					printEchoLine("Application exit!");
					return;
				case "LogLevel":
					logger.info("Loglevel request");
					switchLogLevel(command, line);

				default:
					printEchoLine("Unknown command");
			}
		}
	}

	/**
	 * sendrequest() method that forwards the requests of the client to the correct
	 * server
	 *
	 * @param activeConnection
	 * @param command
	 * @param line
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	private static String sendrequest(ActiveConnection activeConnection, String[] command, String line)
			throws NoSuchAlgorithmException {
		String result = "";
		if (activeConnection == null) {
			printEchoLine("Error! Not connected!");
			result = "Error! Not connected!";

			return result;
		}
		int firstSpace = line.indexOf(" ");
		if ((firstSpace == -1 && (line.length() >= 8 && !line.substring(0, 8).equals("keyrange"))) || firstSpace + 1 >= line.length()) {
			printEchoLine("Error! Nothing to send!");
			result = "Error! Nothing to send!";

			return result;
		}
		activeConnection.write(line);
		// Pause the current thread for a short time so that we wait for the response of
		// the server
		Thread.yield();

		try {
			result = activeConnection.readline();
			// printEchoLine(activeConnection.readline());
			printEchoLine(result);
			return result;

		} catch (IOException e) {
			printEchoLine("Error! Not connected!");
			return "Error! Not connected!";
		}

	}

	private static void printHelp() {
		System.out.println("Available commands:");
		System.out.println(
				"connect <address> <port> - Tries to establish a TCP- connection to the echo server based on the given server address and the port number of the echo service.");
		System.out.println("disconnect - Tries to disconnect from the connected server.");
		System.out.println(
				"send <message> - Sends a text message to the echo server according to the communication protocol.");
		System.out.println(
				"logLevel <level> - Sets the logger to the specified log level (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
		System.out.println("help - Display this help");
		System.out.println("quit - Tears down the active connection to the server and exits the program execution.");
	}

	private static void printEchoLine(String msg) {
		System.out.println("EchoClient> " + msg);
	}

	private static void closeConnection(ActiveConnection activeConnection) {
		if (activeConnection != null) {
			try {
				activeConnection.close();
			} catch (Exception e) {
				// e.printStackTrace();
				// TODO: handle gracefully
				activeConnection = null;
			}
		}
	}

	private static void sendmessage(ActiveConnection activeConnection, String[] command, String line) {
		if (activeConnection == null) {
			printEchoLine("Error! Not connected!");
			return;
		}
		int firstSpace = line.indexOf(" ");
		if (firstSpace == -1 || firstSpace + 1 >= line.length()) {
			printEchoLine("Error! Nothing to send!");
			return;
		}

		String cmd = line.substring(firstSpace + 1);
		activeConnection.write(cmd);
		// Pause the current thread for a short time so that we wait for the response of
		// the server
		Thread.yield();

		try {
			printEchoLine(activeConnection.readline());
		} catch (IOException e) {
			printEchoLine("Error! Not connected!");
		}
	}

	/**
	 * @param command
	 * @return
	 */
	private static ActiveConnection buildconnection(String[] command) {
		if (command.length == 3) {
			try {
				EchoConnectionBuilder kvcb = new EchoConnectionBuilder(command[1], Integer.parseInt(command[2]));
				ActiveConnection ac = kvcb.connect();
				String confirmation = ac.readline();
				printEchoLine(confirmation);
				return ac;
			} catch (Exception e) {
				// Todo: separate between could not connect, unknown host and invalid port
				printEchoLine("Could not connect to server");
			}
		}
		return null;
	}

	/**
	 * getServer() method which takes as parameters the actual metadata of the
	 * client and the hash value of the key and return a Metadata object that
	 * contains the server which is responsible of this key
	 *
	 * @param metadataMap the actual Metadata of the client
	 * @param hash        the hashvalue of the key
	 * @return Metadata object that contains informations about the server which is
	 * responsible of this key
	 */
	private static Metadata getServer(Map<String, Metadata> metadataMap, String hash) {
		Metadata result = null;
		int intHash = (int) Long.parseLong(hash, 16);
		Map<String, Metadata> meta = metadataMap;
		for (Metadata md : meta.values()) {
			int intStart = (int) Long.parseLong(md.getStart(), 16);
			int intEnd = (int) Long.parseLong(md.getEnd(), 16);
			if (intStart < intEnd) {
				if (intHash >= intStart && intHash <= intEnd)
					result = md;

			} else if (intStart > intEnd) {
				if (intHash >= intStart || intHash <= intEnd) {
					result = md;

				}
			}

		}
		return result;

	}

	/*
	 * SEVERE WARNING INFO CONFIG FINE FINER FINEST
	 */

	// to switch the loglevel
	private static void switchLogLevel(String[] command, String line) throws IOException {
		String prevLog = logger.getLevel().toString();
		String newLog = "";
		switch (command[1].toUpperCase()) {
			case "SEVERE":
				logger.setLevel(Level.SEVERE);
				newLog = "SEVERE";
				break;
			case "WARNING":
				logger.setLevel(Level.WARNING);
				newLog = "WARNING";
				break;
			case "INFO":
				logger.setLevel(Level.INFO);
				newLog = "INFO";
				break;
			case "CONFIG":
				logger.setLevel(Level.CONFIG);
				newLog = "CONFIG";
				break;
			case "FINE":
				logger.setLevel(Level.FINE);
				newLog = ".FINE";
				break;
			case "FINER":
				logger.setLevel(Level.FINER);
				newLog = "FINER";
				break;
			case "FINEST":
				logger.setLevel(Level.FINEST);
				newLog = "FINEST";
				break;
			default:
				throw new IOException("Wrong LEVEL input !");

		}
		printEchoLine("loglevel set from " + prevLog + " to " + newLog);

	}

}
