package de.tum.i13.server.circle;

import de.tum.i13.server.threadperconnection.*;
import de.tum.i13.shared.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
// import MD5 for the hashing
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

//import Maven dependency
import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.server.kv.*;
import de.tum.i13.server.threadperconnection.ConnectionHandleThread;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;
import org.apache.commons.codec.binary.Hex;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

//external configuration service
//assigns a position to both servers and Tuples on the ring
public class ECS {

    //Servers repository, also a circular structure? meh we'll see
    LinkedList<Main> serverRepository = new LinkedList<>();

    // chaining servers in a circle
    private Main headServer;
    private Main tailServer;

    //metadata, String is a hashkey
    private Map<String, Metadata> metadataMap = new HashMap<>();

    //One cache to rule them all
    private static Cache cache;

    //buckets gives the number of ranges = number of connected ServerSockets
    private int buckets;

    /*moved is a flag that is set to true when the ranges on the ring must be upadated*/
    boolean moved = false;

    //this method hashes adr and port with md5

    /**
     * hashServer method hashes the IP address and port
     * of a ServerSocket to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    private String hashServer(ServerSocket ss) throws NoSuchAlgorithmException {
        String ip = ss.getInetAddress().getHostAddress();
        String port = String.valueOf(ss.getLocalPort());

        return hashMD5(ip + port);
    }

    /**
     * hashTupel method hashes a given key to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    private String hashMD5(String key) throws NoSuchAlgorithmException {
        byte[] msgToHash = key.getBytes();
        byte[] hashedMsg = MessageDigest.getInstance("MD5").digest(msgToHash);

        //get the result in hexadecimal
        String result = new String(Hex.encodeHex(hashedMsg));
        return result;
    }

    private void addServer(ServerSocket ss) throws NoSuchAlgorithmException, IOException {
        moved = true;
        buckets++;
        int startIndex;     // number if starthash
        String startHash;   // startHash
        Main newMain;       // new added server

        //get hashvalue of a server (ip+port)
        String hash = this.hashServer(ss);

        //getting an index and a hashvalue of a predecessor to be -> startrange

        if (headServer == null) {     // means we have no servers in rep yet
            startIndex = 0;
            //the beginning of th range is an incremented hashvalue
            startHash = Integer.toHexString((int) Long.parseLong(hash, 16) + 1);

            newMain = new Main(cache, startHash, hash);
            this.headServer = newMain;
            this.tailServer = newMain;
            this.tailServer.nextServer = headServer;
        } else {
            Map<Integer, String> indexes = this.locate(hash);
            //findfirst because we have there only one keyvalue :/
            startIndex = (int) indexes.keySet().stream().findFirst().get();//the start of range
            startHash = indexes.get(startIndex);        // already incremented hashvalue
            Main prevServer = this.serverRepository.get(startIndex - 1);

            newMain = new Main(cache, startHash, hash);

            if (this.tailServer == prevServer) {
                this.tailServer = newMain;
                newMain.nextServer = headServer;
            } else {
                newMain.nextServer = prevServer.nextServer;
            }
            prevServer.nextServer = newMain;

            //change next server startrange
            this.serverRepository.get(startIndex + 1).start = Integer.toHexString((int) Long.parseLong(hash, 16) + 1);
            //change prev server endrange
            String endrangeOfPrev = Integer.toHexString((int) Long.parseLong(startHash, 16) - 1);
            prevServer.end = endrangeOfPrev;
        }

        this.metadataMap.put(hash, new Metadata(ss.getInetAddress().getHostAddress(), ss.getLocalPort(), startHash, hash));
        this.serverRepository.add(startIndex, newMain);
        this.updateMetadata();
    }

    private void removeServer(ServerSocket ss) throws Exception {
        moved = true;
        buckets--;

        //locate the server
        Map<Integer, String> indexes = this.locate(this.hashServer(ss));

        //this is the older start value
        int startValue = (int) indexes.keySet().stream().findFirst().get();

        String startHash = indexes.get(startValue);

        /*this is the predecessor = the server that will be responsible for
         the new range (his older range and the range of the deleted server)*/

        Main newRespServer = this.serverRepository.get(startValue - 1);

        //delete the actual server
        //this.serverRepository.remove(startValue); // not possible in circular structure
        //reallocation in the servers repository

        //this means the respository is empty
        if (this.headServer == null) {
            return;
        }
        //find the main to be deleted
        Main tempServer = headServer;

        //if we only have one server
        if (tempServer.equals(headServer) && tempServer.nextServer.equals(headServer)) {
            headServer = null;
            System.out.println("There no servers left"); //maybe exception here ?
        }

        while (!(hashServer(tempServer.getServerSocket()).equals(hashServer(ss)))) {
            //if wrong server is given
            if (tempServer.nextServer.equals(headServer)) {
                System.out.println("Server not found");
                break; //quit because wrong server entered
            }

            //circular structure of serverRespository (normal case)
            newRespServer = tempServer;
            tempServer = tempServer.nextServer;
        }

        //if it is the first server in the ring
        if (tempServer.equals(headServer)) {
            newRespServer = headServer;
            while (!(newRespServer.nextServer.equals(headServer))) {
                newRespServer = newRespServer.nextServer;
            }
            //close the circle
            headServer = tempServer.nextServer;
            newRespServer.nextServer = headServer;
        }

        if (tempServer.nextServer.equals(headServer))
            newRespServer.nextServer = headServer;

        else newRespServer.nextServer = tempServer.nextServer;

//not very sure abt this part
        Metadata newRange = new Metadata(newRespServer.getServerSocket().getInetAddress().toString(), newRespServer.getServerSocket().getLocalPort(), startHash, newRespServer.nextServer.end);

    }


    public boolean shuttingDown() {
        return true;
    }

    public void transferred(boolean check) {

    }

    // find the right location of a new server
    private Map<Integer, String> locate(String hash) {
        Map<Integer, String> returnIndexes = new HashMap();
        int count = 0;
        String previous = "";
        int hashedValue = (int) Long.parseLong(hash, 16);
        //looking for an interval for our new hashed value
        for (Map.Entry element : metadataMap.entrySet()) {
            String hashString = (String) element.getKey();
            int intHash = (int) Long.parseLong(hashString, 16);
            if (hashedValue < intHash) {
                // start index
                returnIndexes.put((Integer) count, previous);
                break;
            }
            count++;
            previous = Integer.toHexString(intHash + 1);
        }
        return returnIndexes;
    }

    //reallocate method should return the updated datastructure
    private void reallocate() {

    }

    //update metadata in servers
    private void updateMetadata() {
        for (Main main : serverRepository) {
            main.setMetadata(metadataMap);
        }
    }

    /**
     * main() method where our serversocket will be initialized
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        ECS ecs = new ECS();

        Config cfg = parseCommandlineArgs(args); // Do not change this
        setupLogging(cfg.logfile);

        //configuring cache for all servers
        if (cfg.cache.equals("FIFO")) {
            cache = new FIFOLRUCache(cfg.cacheSize, false);
        } else if (cfg.cache.equals("LRU")) {
            cache = new FIFOLRUCache(cfg.cacheSize, true);
        } else if (cfg.cache.equals("LFU")) {
            cache = new LFUCache(cfg.cacheSize);
        } else System.out.println("Please check your input for a cache strategy and try again.");

        ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (serverSocket != null)
                        serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // binding to the server through specified bootstrap ip and port
            serverSocket.bind(new InetSocketAddress(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort()));

            while (true) {
                // Waiting for a server to connect
                Socket clientSocket = serverSocket.accept();

                // When we accept a connection, we start a new Thread for this connection
                ECSConnection connection = new ECSConnection(clientSocket, ecs);

                new Thread(connection).start();
            }
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }
}
