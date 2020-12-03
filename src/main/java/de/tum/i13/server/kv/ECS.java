package de.tum.i13.server.kv;

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

    //Servers repository
    LinkedList<Main> serverRepository = new LinkedList<>();

    //metadata
    Map<Main, Metadata> metadataMap = new HashMap<>();

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

    private void addServer(ServerSocket ss) throws NoSuchAlgorithmException {
        moved = true;
        buckets++;
        int startIndex;
        String startHash;

        //get hashvalue of a server (ip+port)
        String hash = this.hashServer(ss);

        //getting an index and a hashvalue of a predessecor to be -> startrange
        //if indexes == null -> we have a first server to add
        Map<Integer, String> indexes = this.locate(hash);
        if(indexes == null){
            startIndex = 0;
            //the beginning of th range is an incremented hashvalue
            startHash = Integer.toHexString((int) Long.parseLong(hash, 16)+1);
        }
        else{
            startIndex = (int) indexes.keySet().stream().findFirst().get()+1;
            startHash = indexes.get(startIndex);
        }

        Main newMain = new Main(cache, startHash, hash);

        this.metadataMap.put(newMain, new Metadata(ss.getInetAddress().getHostAddress(), ss.getLocalPort(), startHash, hash));
        this.serverRepository.add(startIndex, newMain);
    }

    private void removeServer(Main main) {
        moved = true;
        buckets--;

    }

    private Map<Integer, String> locate(String hash){
        Map<Integer, String> returnIndexes = new HashMap();
        int count = 0;
        String previous = "";
        int hashedValue = (int) Long.parseLong(hash, 16);
        //looking for an intervall for our new hashed value
        for(Map.Entry element : metadataMap.entrySet()){
            String hashString = (String) element.getKey();
            int intHash = (int) Long.parseLong(hashString, 16);
            if (hashedValue < intHash){
                // start index
                returnIndexes.put((Integer) count, previous);
                break;
            }
            count++;
            previous = Integer.toHexString(intHash+1);
        }
        return returnIndexes;
    }

    private void reallocate() {

    }

    private void updateMetadata() {

    }

    /**
     * main() method where our serversocket will be initialized
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args); // Do not change this
        setupLogging(cfg.logfile);

        //configuring cache for all servers
        if (cfg.cache.equals("FIFO")){
            cache = new FIFOLRUCache(cfg.cacheSize, false);
        }
        else if(cfg.cache.equals("LRU")){
            cache = new FIFOLRUCache(cfg.cacheSize, true);
        }
        else if(cfg.cache.equals("LFU")){
            cache = new LFUCache(cfg.cacheSize);
        }
        else System.out.println("Please check your input for a cache strategy and try again.");

        final ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Closing thread per connection kv server");
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        // binding to the server through specified bootstrap ip and port
        serverSocket.bind(new InetSocketAddress(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort()));

        while (true) {
            // Waiting for a server to connect
            Socket clientSocket = serverSocket.accept();

            // When we accept a connection, we start a new Thread for this connection
            Thread th = new ConnectionHandleThread(logic, clientSocket);
            th.start();
        }

    }
}