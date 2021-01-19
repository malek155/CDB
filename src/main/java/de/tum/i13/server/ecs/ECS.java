package de.tum.i13.server.ecs;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Logger;

//import Maven dependency
import de.tum.i13.server.threadperconnection.Main;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

//external configuration service
public class ECS {
    private String newServer;
    private String nextHash;
    private String prevHash;
    private String nextNextHash;
    private boolean newlyAdded;
    public ArrayList<ECSConnection> connections = new ArrayList<>();

    //Servers repository, also a circular structure? meh we'll see
    private LinkedList<Main> serverRepository = new LinkedList<>();

    // chaining servers in a ecs
    private Main headServer;
    private Main tailServer;

    //metadata, String is a hashkey
    private static TreeMap<String, Metadata> metadataMap = new TreeMap<>();

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
        String myHash = new BigInteger(1, digested).toString(16);

        return myHash;
    }

    /**
     * addServer method adds a server to serverRepository, its data to metadataMap, updates circular relationships
     * @param ip, port are credentials of a new server
     */
    public void addServer(String ip, int port) throws NoSuchAlgorithmException{
        logger.info("started adding a server");
        int startIndex;     // number if starthash
        String startHash;   // startHash
        Main newMain = new Main();       // new added server

        Main prevServer = null;

        String hash = this.hashMD5(ip+port);

        //getting an index and a hashvalue of a predecessor to be -> startrange
        if (headServer == null) {     // means we have no servers in rep yet
            startIndex = 0;
            //the beginning of th range is an incremented hashvalue

            startHash = this.arithmeticHash(hash, true);
            newMain.end = hash;
            newMain.start = this.arithmeticHash(hash, true);
            newMain.nextServer = newMain;
            this.headServer = newMain;
            this.tailServer = newMain;
        } else {
            Map<Integer, String> indexes = this.locate(hash);
            //findfirst because we have there only one keyvalue :/
            startIndex = indexes.keySet().stream().findFirst().get();

            // checking if we're in the beginning of the circle -> end smaller than start
            startHash = (startIndex == 0)
                ? this.arithmeticHash(tailServer.end, true)
                : indexes.get(startIndex);        // already incremented hashvalue

            newMain.start = startHash;
            newMain.end = hash;

            prevServer = (startIndex == 0)
                ? serverRepository.getLast()
                : this.serverRepository.get(startIndex - 1);

            if (this.tailServer == prevServer && startIndex != 0) {
                newMain.nextServer = headServer;
                this.tailServer = newMain;
            }else if(startIndex == 0){
                newMain.nextServer = serverRepository.getLast().nextServer;
                this.headServer = newMain;
                this.tailServer.nextServer = newMain;
            }else{
                newMain.nextServer = prevServer.nextServer;
            }
            //change next server startrange
            if(startIndex==serverRepository.size())
                this.serverRepository.get(startIndex-1).start = this.arithmeticHash(hash, true);
            else
                this.serverRepository.get(startIndex).start = this.arithmeticHash(hash, true);

            //change start of a next server in metadata
            Metadata nextMeta = metadataMap.get(newMain.nextServer.end);
            metadataMap.put(nextMeta.getEnd(), new Metadata(nextMeta.getIP(), nextMeta.getPort(), this.arithmeticHash(hash, true), nextMeta.getEnd()));
        }
        metadataMap.put(hash, new Metadata(ip, port, startHash, hash));
        this.serverRepository.add(startIndex, newMain);

        // for updating metadata
        this.moved = true;

        //for ecs connection, boolean if a new server was added
        newlyAdded = true;
        newServer = hash;
        nextHash = newMain.nextServer.end;
        nextNextHash = newMain.nextServer.nextServer.end;
        if(prevServer != null) prevHash = prevServer.end;

        logger.info("Added a new server, listening on " + ip + ":" + port);
    }

    /**
     * removeServer method deletes a server from the serverRepository and
     * deletes its data from metadataMap, updates circular relationships
     *
     * @param (ip,port) are credentials for the server to remove
     */
    private void removeServer(String ip, int port) throws Exception {
        if(serverRepository.size() == 1){
            serverRepository.remove(0);
            metadataMap.clear();
            headServer = null;
            tailServer = null;
            logger.info("There are no servers left");
        }
        else if(serverRepository.size() == 0){
            logger.warning("Server repository is already empty");
            return;
        }

        String hash = this.hashMD5(ip + port);

        int indexToRemove = this.locate(hash).keySet().stream().findFirst().get();


        String prevHash = (indexToRemove == 0)
                ? tailServer.end
                : String.valueOf(serverRepository.get(indexToRemove-1));        // hash of a prev server, ugly ugly

        String nextHash = String.valueOf(serverRepository.get(indexToRemove+1));

        if(indexToRemove == 0){
            headServer = headServer.nextServer;
            tailServer.nextServer = tailServer;
        }
        else if(indexToRemove == serverRepository.size()){
            tailServer = serverRepository.get(indexToRemove-1);
            tailServer.nextServer = headServer;
        }
        else{
            serverRepository.get(indexToRemove-1).nextServer = serverRepository.get(indexToRemove+1);
        }

        this.removeConnection(ip, port);

        //reallocating metadata
        metadataMap.get(prevHash).setEnd(metadataMap.get(hash).getEnd());
        metadataMap.get(nextHash).setStart(metadataMap.get(hash).getStart());
        metadataMap.remove(hash);

        //reallocating server repository
        serverRepository.remove(indexToRemove);

        this.moved = true;

        logger.info("Removed a server, listening on: " + ip + ":" + port);
    }

    /**
     * shuttingDown method reallocates the servers and then returns further instructions
     * @param hash is a hashed value of a server-to-remove
     * @return String, a hash of a receiving server
     */
    public ArrayList<String> shuttingDown(String ip, int port, String hash) throws Exception {
        Map<Integer, String> indexes = this.locate(hash);
        // we get the index of a previous neighbour of server-to-remove -> +1 to get next one
        int current = indexes.keySet().stream().findFirst().get();
        int next = current++;
        int nextNext = next++;
        ArrayList<String> neighbours = new ArrayList<>();

        neighbours.add(hash);
        neighbours.add(serverRepository.get(next).end);
        neighbours.add(serverRepository.get(nextNext).end);

        this.removeServer(ip, port);
        logger.info("Approving shutting down of a server, rebalancing is in the process");
        return neighbours;
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
        BigInteger hashToLocate = new BigInteger(hash, 16);

        String hashToCmpString;
        BigInteger hashToCmp;

        //looking for an interval for our new hashed value || a hash to remove
        for (Map.Entry element : metadataMap.entrySet()) {
            hashToCmpString = (String) element.getKey();
            hashToCmp = new BigInteger(hashToCmpString, 16);

            if (hashToLocate.compareTo(hashToCmp)<=0) {
                returnIndexes.put(count, startRange);
                break;
            }
            count++;
            startRange = arithmeticHash(hashToCmpString, true);
            if(hashToCmpString.equals(metadataMap.lastKey())){
                returnIndexes.put(count, startRange);
                break;
            }
        }
        return returnIndexes;
    }

    /**
     * isAdded method submits (or not), that we already have this server in a repository
     * @param ip, port of a possible server to add
     * @return boolean: true if already existing
     */
    public boolean isAdded(String ip, int port){
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

    public void movedMeta(){
        for(ECSConnection ecsConnection : connections)
            ecsConnection.sendMeta();
        this.setMoved(false);
        logger.info("Updating metadata in servers");
    }

    public void notifyServers(String current, String next, String nextNext){

        for (ECSConnection ecsConnection : connections) {
            if (next.equals("") && nextNext.equals("") && current.equals(""))
                ecsConnection.reallocate();
            else if(ecsConnection.getHash().equals(next) || ecsConnection.getHash().equals(nextNext))
                ecsConnection.notifyIfDelete(current, next, nextNext);
        }
        this.newlyAdded = false;
    }

    private void removeConnection(String ip, int port){
        for(ECSConnection ecsConnection : connections) {
            if (ecsConnection.getIP().equals(ip) && ecsConnection.getPort() == port)
                connections.remove(ecsConnection);
        }
    }

    /**
     * setMoved method sets the boolean "moved" for consistent updating of metadata
     *
     * @param update tells if we need to update metadata or not
     */
    public void setMoved(boolean update) {
        this.moved = update;
    }

    private String arithmeticHash(String hash, boolean increment){
        BigInteger bigHash = new BigInteger(hash, 16);
        bigHash = (increment) ? bigHash.add(BigInteger.ONE) : bigHash.subtract(BigInteger.ONE);
        return bigHash.toString(16);
    }

    public boolean getMoved(){return moved;}

    public Map<String, Metadata> getMetadataMap(){
        return metadataMap;
    }

    public String getNewServer(){return newServer;}

    public String getNextHash(){return nextHash;}

    public String getPrevHash(){return prevHash;}

    public String getNextNextHash(){return nextNextHash;}

    public boolean isNewlyAdded(){return this.newlyAdded;}

    public void setNewlyAdded(boolean newly){this.newlyAdded = newly;}

    public LinkedList<Main> getServerRepository(){return this.serverRepository;}

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

        logger.info("initialized the ECS");
        try {
            // binding to the server through specified bootstrap ip and port
//            serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));
            serverSocket.bind(new InetSocketAddress(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort()));

            while (true){
                // Waiting for a server to connect
                Socket clientSocket = serverSocket.accept();

                // When we accept a connection, we start a new Thread for this connection
                ECSConnection connection = new ECSConnection(clientSocket, ecs);
                ecs.connections.add(connection);

                new Thread(connection).start();
            }
        }catch(IOException | NoSuchAlgorithmException ie){
            ie.printStackTrace();
        }
    }

}





