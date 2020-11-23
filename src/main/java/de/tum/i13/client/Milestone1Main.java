package de.tum.i13.client;

import de.tum.i13.server.kv.KVStoreProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;


public class Milestone1Main {
    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        ActiveConnection activeConnection = null;
        KVStoreProcessor kvStoreProcessor = new KVStoreProcessor();
        KVCommandProcessor kvCommandProcessor = new KVCommandProcessor(kvStoreProcessor);
        for(;;) {
            System.out.print("EchoClient> ");
            String line = reader.readLine();
            String[] command = line.split(" ");
            //System.out.print("command:");
            //System.out.println(line);
            switch (command[0]) {
                case "connect": activeConnection = buildconnection(command); break;
                case "send": sendmessage(activeConnection, command, line); break;
                case "disconnect": closeConnection(activeConnection); break;
                case "help": printHelp(); break;
                case "quit": printEchoLine("Application exit!"); return;
                case "put":
                case "get": kvFunction(line, kvCommandProcessor); break;
                default: printEchoLine("Unknown command");
            }
        }
    }

    private static void kvFunction(String line, KVCommandProcessor kvCommandProcessor){
        kvCommandProcessor.process(line);
    }

    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("connect <address> <port> - Tries to establish a TCP- connection to the echo server based on the given server address and the port number of the echo service.");
        System.out.println("disconnect - Tries to disconnect from the connected server.");
        System.out.println("send <message> - Sends a text message to the echo server according to the communication protocol.");
        System.out.println("logLevel <level> - Sets the logger to the specified log level (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
        System.out.println("help - Display this help");
        System.out.println("quit - Tears down the active connection to the server and exits the program execution.");
    }

    private static void printEchoLine(String msg) {
        System.out.println("EchoClient> " + msg);
    }

    private static void closeConnection(ActiveConnection activeConnection) {
        if(activeConnection != null) {
            try {
                activeConnection.close();
            } catch (SocketException e) {
                //TODO: handle gracefully
                activeConnection = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendmessage(ActiveConnection activeConnection, String[] command, String line) {
        if(activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }
        int firstSpace = line.indexOf(" ");
        if(firstSpace == -1 || firstSpace + 1 >= line.length()) {
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
        if(command.length == 3){
            try {
                EchoConnectionBuilder kvcb = new EchoConnectionBuilder(command[1], Integer.parseInt(command[2]));
                ActiveConnection ac = kvcb.connect();
                String confirmation = ac.readline();
                printEchoLine(confirmation);
                return ac;
            } catch (Exception e) {
                //Todo: separate between could not connect, unknown host and invalid port
                printEchoLine("Could not connect to server");
            }
        }
        return null;
    }
}