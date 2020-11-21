package de.tum.i13.server.echo;

import de.tum.i13.server.kv.KVStoreProcessor;
import de.tum.i13.shared.CommandProcessor;

import java.util.Collections;
import java.util.logging.Logger;
import java.io.IOException;
import java.net.*;

public class EchoLogic implements CommandProcessor {
    public static Logger logger = Logger.getLogger(EchoLogic.class.getName());
    KVStoreProcessor kvp = new KVStoreProcessor();
    public static ServerSocket serverSocket;


    // Sarra: implementation of command line parameters
    public String process(String command) {

        logger.info("received command: " + command.trim());

        // Let the magic happen here

        switch (command.trim().substring(0, 2)) {
            case "-p":
                setServerPort(Integer.parseInt(command.trim().substring(2)));
                break;
            case "-a":
                setServerAddress(command.trim().substring(2));
                break;
            case "-b":
                ;
                break;
            case "-d":
                ;
                break;
            case "-l":
                ;
                break;
            case "-c":
                kvp.getCache().setSize(Integer.parseInt(command.trim().substring(2)));
                break;
            case "-s":
                setCacheDispalcement(command.trim().substring(2));
                break;
            case "-h":
                printHelp();
                break;
        }
        return command;
    }

    /**
     *
     */
    public void setServerPort(int port) {
        try {
            serverSocket = new ServerSocket(port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("Host is unknown");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error");
        }
    }


    public void setServerAddress(String IPaddress) {
        //the second argument is the backlog= max number of clients
        try {
            serverSocket = new ServerSocket(0, 1000, InetAddress.getByName(IPaddress));
        } catch (IOException e) {
            try {
                serverSocket = new ServerSocket(0, 1000, InetAddress.getByName("127.0.0.1"));
            } catch (IOException ioException) {
                ioException.printStackTrace();
                System.out.println("IP Address not recognized");
            }

        }
    }
// i gave up on this one
    public void setCacheDispalcement(String strategy) {
        if (strategy.toLowerCase().equals("fifo")||strategy.toLowerCase().equals("lru")){

        }
        else if (strategy.toLowerCase().equals("lfu")){

        }
        else return;
    }

    public void printHelp() {
        System.out.println("Available commands:");
        System.out.println("connect <address> <port> - Tries to establish a TCP- connection to the echo server based on the given server address and the port number of the echo service.");
        System.out.println("disconnect - Tries to disconnect from the connected server.");
        System.out.println("send <message> - Sends a text message to the echo server according to the communication protocol.");
        System.out.println("logLevel <level> - Sets the logger to the specified log level (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
        System.out.println("help - Display this help");
        System.out.println("quit - Tears down the active connection to the server and exits the program execution.");
    }

    //public int cacheSize() {
    //   return kvp.getCache().getSize();
    //}

    public static void main(String[] args) {
        logger.setLevel(null);
        System.out.println(logger.getName());
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());

        return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress remoteAddress) {
        logger.info("connection closed: " + remoteAddress.toString());
    }
}