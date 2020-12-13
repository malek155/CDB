package de.tum.i13.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;

import de.tum.i13.shared.Metadata;

public class Milestone1Main {
    private  Map<String, Metadata> metadataMap = new HashMap<>();
    /**
     * hashKey method hashes the key a keyvalue to its
     * Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    private String hashKey(String key) throws NoSuchAlgorithmException {


        return hashMD5(key);
    }

    /**
     * hashTupel method hashes a given key to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    private String hashMD5(String key) throws NoSuchAlgorithmException {
        byte[] msgToHash = key.getBytes();
        byte[] hashedMsg = MessageDigest.getInstance("MD5").digest(msgToHash);

        // get the result in hexadecimal
        String result = new String(Hex.encodeHex(hashedMsg));
        return result;
    }

    public static void main(String[] args) throws IOException {

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

                    sendrequest(activeConnection, command, line);
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
                // Todo: separate between could not connect, unknown host and invalid port
                printEchoLine("Could not connect to server");
            }
        }
        return null;
    }

    // we need a method where we give the key and the map of the metadata and it
    // returns a ServerSocket containing the server which is responsible of this key
    // and then we compare it to the server that we are already connected to and if
    // it is not the same we reconnect to the appropriate server and resend the last
    // request
}