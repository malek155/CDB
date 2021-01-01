package de.tum.i13.server.ecs;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Logger;

//import Maven dependency
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

//external configuration service
public class ECS {
    private String newServer;
    private String neighbourHash;
    private boolean newlyAdded;

    //Servers repository, also a circular structure? meh we'll see
    private LinkedList<Main> serverRepository = new LinkedList<>();

    // chaining servers in a ecs
    private Main headServer;
    private Main tailServer;

    //metadata, String is a hashkey
    private static Map<String, Metadata> metadataMap = new HashMap<>();

    /*moved is a flag that is set to true when the ranges on the ring must be updated*/
    private boolean moved;

    public static Logger logger = Logger.getLogger(ECS.class.getName());

    /**
     * hashMD5 method hashes a given key to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    public String hashMD5(String key) throws NoSuchAlgorithmException {
        MessageDigest msg = MessageDigest.getInstance("MD5");
        byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));

        return new String(digested);
    }

    /**
     * addServer method adds a server to serverRepository, its data to metadataMap, updates circular relationships
     * @param ip, port are credentials of a new server
     */
    private void addServer(String ip, int port) throws NoSuchAlgorithmException {
        moved = true;
        int startIndex;     // number if starthash
        String startHash;   // startHash
        Main newMain = new Main();;       // new added server

        String hash = this.hashMD5(ip+port);

        //getting an index and a hashvalue of a predecessor to be -> startrange
        if (headServer == null) {     // means we have no servers in rep yet
            startIndex = 0;
            //the beginning of th range is an incremented hashvalue
            startHash = Integer.toHexString((int) Long.parseLong(hash, 16) + 1);

            this.headServer = newMain;
            this.tailServer = newMain;
            this.tailServer.nextServer = headServer;
        } else {
            Map<Integer, String> indexes = this.locate(hash);
            //findfirst because we have there only one keyvalue :/
            startIndex = indexes.keySet().stream().findFirst().get();

            // checking if we're in the beginning of the circle -> end smaller than start
            startHash = (startIndex == 0)
                ? Integer.toHexString((int) Long.parseLong(tailServer.end, 16) + 1)
                : indexes.get(startIndex);        // already incremented hashvalue

            Main prevServer = (startIndex == 0)
                ? serverRepository.getLast()
                : this.serverRepository.get(startIndex - 1);

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

        metadataMap.put(hash, new Metadata(ip, port, startHash, hash));
        this.serverRepository.add(startIndex, newMain);

        //for ecs connection
        newlyAdded = true;
        newServer = hash;
        neighbourHash = newMain.nextServer.end;

        logger.info("Added a new server, listening on " + ip + ":" + port);
    }

    /**
     * removeServer method deletes a server from the serverRepository and
     * deletes its data from metadataMap, updates circular relationships
     *
     * @param (ip,port) are credentials for the server to remove
     */
    private void removeServer(String ip, int port) throws Exception {
        moved = true;

        String hash = this.hashMD5(ip + port);

        Map<Integer, String> returnIndexes = new HashMap();

        Metadata mdToRemove = null;
        String newStart = null;

        //count is the index of the next server (the new responsible server)
        int count = 1;

        for (Map.Entry entry : metadataMap.entrySet()) {
            count++;
            if (entry.getKey().toString().equals(hashMD5(ip + port))) {
                //the metadata of the server to be removed
                mdToRemove = (Metadata) entry.getValue();
                newStart = mdToRemove.getStart();
                //remove the metadata
                mdToRemove = null;
                break;
            }
        }

//updating the metadata of the next server
        metadataMap.get(count).setStart(newStart);

        //removing the main in server repository
        Main predMain = null;

        //find the main to be deleted
        Main tempServer = headServer;

        //if respository is empty
        if (this.headServer == null) {
            return;
        }

        //if we only have one server in the ring
        if (tempServer.equals(headServer) && tempServer.nextServer.equals(headServer)) {
            headServer = null;
            System.out.println("There are no servers left"); //maybe exception here ?
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

        logger.info("Removed a server, listening on: " + ip + ":" + port);
    }

    /**
     * shuttingDown method reallocates the servers and then returns further instructions
     * @param hash is a hashed value of a server-to-remove
     * @return String, a hash of a receiving server
     */
    public String shuttingDown(String ip, int port, String hash) throws Exception {
        Map<Integer, String> indexes = this.locate(hash);
        this.removeServer(ip, port);
        // we get the index of a previous neighbour of server-to-remove -> +2 to get next one
        int thankUnext = indexes.keySet().stream().findFirst().get() + 2;

        logger.info("Approving shutting down of a server, rebalancing is in the process");
        return serverRepository.get(thankUnext).end;
    }

    // find the right location of a new server
    /**
     * locate method locates the position, where we should add a new server or helps to locate a definite serverindex
     * @param hash of a server to add / find
     * @return Map<Integer, String>
     *      by adding:  Integer is responsible for N(natural) index of a server-to-add
     *                  String is responsible for hashValue of previous Server+1 -> startHash of a server-to-add
     *      by finding: Integer is responsible for N(natural) index of a previous server
     */
    private Map<Integer, String> locate(String hash){
        Map<Integer, String> returnIndexes = new HashMap();
        int count = 0;
        String startRange = "";
        int hashedValue = (int) Long.parseLong(hash, 16);
        //looking for an interval for our new hashed value
        for (Map.Entry element : metadataMap.entrySet()) {
            String hashString = (String) element.getKey();
            int intHash = (int) Long.parseLong(hashString, 16);
            if (hashedValue <= intHash) {
                // start index
                returnIndexes.put(count, startRange);
                break;
            }
            count++;
            startRange = Integer.toHexString(intHash + 1);
        }
        return returnIndexes;
    }

    /**
     * isAdded method submits (or not), that we already have this server in a repository
     * @param ip, port of a possible server to add
     * @return boolean: true if already existing
     */
    private boolean isAdded(String ip, int port){
        boolean added = false;
        for (Map.Entry element : metadataMap.entrySet()) {
            Metadata metadata = (Metadata) element.getValue();
            if(metadata.getIP().equals(ip) && metadata.getPort()==port){
                added = true;
                break;
            }
        }
        return added;
    }

    /**
     * setMoved method sets the boolean "moved" for consistent updating of metadata
     *
     * @param update tells if we need to update metadata or not
     */
    public void setMoved(boolean update) {
        this.moved = update;
    }

    public boolean getMoved(){return moved;}

    public Map<String, Metadata> getMetadataMap(){
        return metadataMap;
    }

    public String getNewServer(){return newServer;}

    public String getNeighbourHash(){return neighbourHash;}

    public boolean isNewlyAdded(){return this.newlyAdded;}

    public void setNewlyAdded(boolean newly){this.newlyAdded = newly;}

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

        ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if(serverSocket != null)
                        serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // binding to the server through specified bootstrap ip and port
            serverSocket.bind(new InetSocketAddress(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort()));

            while (true){
                // Waiting for a server to connect
                Socket clientSocket = serverSocket.accept();

                // When we accept a connection, we start a new Thread for this connection
                ECSConnection connection = new ECSConnection(clientSocket, ecs);

                new Thread(connection).start();

                if(!ecs.isAdded(cfg.listenaddr, cfg.port)){
                    ecs.addServer(cfg.listenaddr, cfg.port);
                }
            }
        }catch(IOException | NoSuchAlgorithmException ie){
            ie.printStackTrace();
        }
    }
}





