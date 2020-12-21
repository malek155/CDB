package de.tum.i13.client;


import de.tum.i13.shared.Metadata;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class Milestone1Main {

    private Map<String, Metadata> metadataMap = new HashMap<>();

    /**
     * hashTupel method hashes a given key to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    public String hashMD5(String key) throws NoSuchAlgorithmException {

        MessageDigest msg = MessageDigest.getInstance("MD5");
        byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));

        return new String(digested);
    }

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        ActiveConnection activeConnection = null;
        for (;;) {
            System.out.print("EchoClient> ");
            String line = reader.readLine();
            String[] command = line.split(" ");

            switch (command[0]) {
                case "connect":
                    activeConnection = buildconnection(command);
                    break;
                case "send":
                    sendmessage(activeConnection, command, line);
                    break;
                case "put":
                case "get":
                    int count = 0;
                    while (true) {
                        sendrequest(activeConnection, command, line);
                        String result = activeConnection.readline();
                        if (result.equals("server_write_lock") || result.equals("server_stopped")) {
                            count++;
                        }
                    }
//				break;

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

    private static void sendrequest(ActiveConnection activeConnection, String[] command, String line) {
        if (activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }
        int firstSpace = line.indexOf(" ");
        if (firstSpace == -1 || firstSpace + 1 >= line.length()) {
            printEchoLine("Error! Nothing to send!");
            return;
        }

        activeConnection.write(line);

        try {
            printEchoLine(activeConnection.readline());
        } catch (IOException e) {
            printEchoLine("Error! Not connected!");
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
                printEchoLine(e.getMessage());
            }
        }
        return null;
    }
}