package de.tum.i13.server.circle;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
// import MD5 for the hashing
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

//import Maven dependency
import de.tum.i13.server.kv.*;
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

    // chaining servers in a ecs
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
            startIndex = (int) indexes.keySet().stream().findFirst().get();
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

        //locate the server to be removed
        Map<Integer, String> returnIndexes = new HashMap();
        Metadata mdToRemove = null;
        Metadata mdNext = null;

        //count is used to define the next server
        int count = 0;
        for (Map.Entry entry : metadataMap.entrySet()) {
            count++;
            if (entry.getKey().toString().equals(hashServer(ss))) {
                //the metadata of the server to be removed
                mdToRemove = (Metadata) entry.getValue();
            }
        }
        //end>start
        String startToRemove = mdToRemove.getStart();
        String newEnd = mdToRemove.getEnd();

        //case differentiation

        //removing the server
        metadataMap.remove(hashServer(ss));

        //updating the metadata of the next server IN THE METADATA
        metadataMap.get(count + 1).setEnd(newEnd);


        //now deleting the mains in server respository
        Main predMain = null;
        //find the main to be deleted
        Main tempServer = headServer;
        while (!(tempServer.getNextIP().equals(ss.getInetAddress()))){
            predMain=tempServer;
            tempServer=tempServer.nextServer;
            //if ss isn't found
            if (tempServer.nextServer.equals(headServer)){
                System.out.println("Server to be removed not in the repository");
                break;//quit because wrong server entered
            }
        }
        //Main newRespServer = new Main(cache, hashMD5(predMain.getServerSocket().getInetAddress().toString()), hashMD5(ss.getInetAddress().toString()));

        //delete the actual server
        //this.serverRepository.remove(startValue); // not possible in circular structure

        //if respository is empty
        if (this.headServer == null) {
            return;
        }

        //if we only have one server in the ring
        if (tempServer.equals(headServer) && tempServer.nextServer.equals(headServer)) {
            headServer = null;
            System.out.println("There no servers left"); //maybe exception here ?
            //where does the storage go?
        }


        //if ss is the first server in the ring
        if (tempServer.equals(headServer)) {
            predMain = headServer;
            while (!(predMain.nextServer.equals(headServer))) {
                predMain = predMain.nextServer;
            }
            //close the circle
            headServer = tempServer.nextServer;
            predMain.nextServer = headServer;
        }

        //if ss is the last server in the ring
        if (tempServer.nextServer.equals(headServer))
            predMain.nextServer = headServer;

        //if ss in the middle (normal case)
        else predMain.nextServer = tempServer.nextServer;


    }


    public boolean shuttingDown() {
        return true;
    }

    public void transferred(boolean check) {

    }

    //gave up on trying to understand the logic of this
    // find the right location of a new server
    private Map<Integer, String> locate(String hash) {
        //integer count mtaa eli 9ablou
        //string serveur ejdid men yebda, from mtaa jdid

        Map<Integer, String> returnIndexes = new HashMap();
        int count = 0;
        String previous = "";

        //value of md5 hex in integer
        int hashedValue = (int) Long.parseLong(hash, 16);

        //looking for an interval for our new hashed value
        for (Map.Entry element : metadataMap.entrySet()) {
            String hashString = (String) element.getKey();

            //going through the hashvalues of the elements
            int intHash = (int) Long.parseLong(hashString, 16);
            if (hashedValue < intHash) {
                // start index
                returnIndexes.put((Integer) count, previous);
                break;
            }
            //eli 9bal
            count++;
            previous = Integer.toHexString(intHash + 1);
        }
        return returnIndexes;
    }

    //reallocation done when a server is removed but i guess we can change it idk
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
