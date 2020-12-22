package de.tum.i13.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import de.tum.i13.shared.Metadata;

public class Milestone1Main {

	/**
	 * hashMD5 method hashes a given key to its Hexadecimal value with md5
	 *
	 * @return String of hashvalue in Hexadecimal
	 */
	public static String hashMD5(String key) throws NoSuchAlgorithmException {

		MessageDigest msg = MessageDigest.getInstance("MD5");
		byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));
		return new String(digested);

	}

	public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException {
		Map<String, Metadata> metadataMap = new HashMap<>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		ActiveConnection activeConnection = null;

		for (;;) {
			System.out.print("EchoClient> ");
			String line = reader.readLine();
			String[] command = line.split(" ");
			// System.out.print("command:");
			// System.out.println(line);
			switch (command[0]) {
				case "connect":
					activeConnection = buildconnection(command);
					break;
				case "send":
					sendmessage(activeConnection, command, line);
					break;
				case "put":
				case "get":
				case "delete":
					// number of retry
					int count = 0;
					// the maximum number of retry is 5
					while (count <= 5) {
						// we check whether the metadata is empty
						if (!metadataMap.isEmpty()) {
							Metadata meta = null;
							try {
								// getting the server which is responsible of this key
								meta = getServer(metadataMap, hashMD5(command[1]));
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}
							String[] a = { null, meta.getIP(), "" + meta.getPort() };
							// building a new connection to this server
							activeConnection = buildconnection(a);

						}
						// send the request
						String result = sendrequest(activeConnection, command, line);
						// if we get a server_write lock or server_stopped notification from the server
						// we weet for some time and we retry
						if (result.equals("server_write_lock") || result.equals("server_stopped")) {
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
							Metadata meta = null;
							try {
								// getting the server which is responsible of this key
								meta = getServer(metadataMap, hashMD5(command[1]));
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}
							String[] a = { null, meta.getIP(), "" + meta.getPort() };
							// building a new connection to this server
							activeConnection = buildconnection(a);
							// retry the request to the new server
							sendrequest(activeConnection, command, line);
							// updating count
							count = 0;

						}
					}

					break;

				case "disconnect":
					closeConnection(activeConnection);
					break;
				case "help":
					printHelp();
					break;
				case "quit":
					printEchoLine("Application exit!");
					return;
				default:
					printEchoLine("Unknown command");
			}
		}
	}

	private static String sendrequest(ActiveConnection activeConnection, String[] command, String line)
			throws NoSuchAlgorithmException {
		String result = "";
		if (activeConnection == null) {
			printEchoLine("Error! Not connected!");
			result = "Error! Not connected!";
			// return ;
			return result;
		}
		int firstSpace = line.indexOf(" ");
		if (firstSpace == -1 || firstSpace + 1 >= line.length()) {
			printEchoLine("Error! Nothing to send!");
			result = "Error! Nothing to send!";
			// return;
			return result;
		}

		activeConnection.write(line + " " + hashMD5(command[1]));
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
	 *         responsible of this key
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

}
