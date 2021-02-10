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
import de.tum.i13.server.kv.Broker;
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
    private String nextNextNextHash;
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
        newServer = hash;

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

            if (this.tailServer == prevServer && startIndex != 0){
                newMain.nextServer = headServer;
                this.tailServer.nextServer = newMain;
                this.tailServer = newMain;
            }else if(startIndex==0 &&  indexes.get(startIndex).equals("")){
                headServer.nextServer = newMain;
                headServer.start = arithmeticHash(hash, true);
                newMain.nextServer = headServer;
                newMain.start = arithmeticHash(headServer.end, true);
                tailServer = newMain;
            }
            else if(startIndex == 0){
                newMain.nextServer = serverRepository.getLast().nextServer;
                logger.info(newMain.nextServer.end);
                this.headServer = newMain;
                this.tailServer.nextServer = newMain;
            }else{
                newMain.nextServer = prevServer.nextServer;
                prevServer.nextServer = newMain;
            }
            //change next server startrange
            // why not -1? because we got through the whole loop and have startindex, that does not exit yet
            if(startIndex==serverRepository.size())
                this.serverRepository.getFirst().start = this.arithmeticHash(newMain.end, true);
            else  // next one, because server rep not updated yet
                this.serverRepository.get(startIndex).start = this.arithmeticHash(hash, true);

            //change start of a next server in metadata
            Metadata nextMeta = metadataMap.get(newMain.nextServer.end);
            metadataMap.put(nextMeta.getEnd(), new Metadata(nextMeta.getIP(), nextMeta.getPort(), this.arithmeticHash(hash, true), nextMeta.getEnd()));
        }
        metadataMap.put(hash, new Metadata(ip, port, startHash, hash));
        this.serverRepository.add(startIndex, newMain);

        // for updating metadata
        this.movedMeta();

        nextHash = newMain.nextServer.end;
        nextNextHash = newMain.nextServer.nextServer.end;
        nextNextNextHash = newMain.nextServer.nextServer.nextServer.end;
        if(prevServer != null) this.prevHash = prevServer.end;

        //for ecs connection, boolean if a new server was added
        if (this.serverRepository.size() > 1){
            this.notifyServers("", "", "");
            logger.info("Notifying a server, that it needs to send a data to a new server");
        }

        logger.info("Added a new server, listening on " + ip + ":" + port);
    }

    /**
     * removeServer method deletes a server from the serverRepository and
     * deletes its data from metadataMap, ecsConnections - updates circular relationships
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

//        this.moved = true;
        this.movedMeta();
        //we have it updated right from ecs, we don't check for a flag in ecsconnection anymore

        logger.info("Removed a server, listening on: " + ip + ":" + port);
    }

    /**
     * shuttingDown method reallocates the servers and then returns further instructions
     * @param hash is a hashed value of a server-to-remove
     * @return String, a hash of a receiving server
     */
    public void shuttingDown(String ip, int port, String hash) throws Exception {
        Map<Integer, String> indexes = this.locate(hash);
        // we get the index of a previous neighbour of server-to-remove -> +1 to get next one
        int current = indexes.keySet().stream().findFirst().get();
        int next;
        int nextNext;

        String nextHash = "";
        String nextNextHash = "";
        ArrayList<String> neighbours = new ArrayList<>();
        neighbours.add(hash);

        if(serverRepository.size() > 1){
            next = current++;
            nextHash = serverRepository.get(next).end;
            if(serverRepository.size()>2){
                nextNext = next++;
                nextNextHash = serverRepository.get(nextNext).end;
            }
        }

        this.notifyServers(hash, nextHash, nextNextHash);
        this.removeServer(ip, port);
        logger.info("Approving shutting down of a server, rebalancing is in the process");
    }

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
            startRange = arithmeticHash(hashToCmpString, true);
            if(hashToCmpString.equals(metadataMap.lastKey())){
                returnIndexes.put(count, startRange);
                break;
            }
            count++;
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

    /**
     * movedMeta method invoked from ecsConnection to send all the servers updated metadata
     */
    public void movedMeta(){
        for(ECSConnection ecsConnection : connections)
            ecsConnection.sendMeta();
        this.setMoved(false);
        logger.info("Updating metadata in servers");
    }

    /**
     * notifyServers method invoked from ecsConnection to send all servers new info about a new server to reallocate data
     * if parameters empty
     *                                                 to send a server-to-delete, its next and next after next neighbours a notification
     * to reallocate their data
     *
     * @param current a server-to-remove
     *  @param next next server, handling replicas and a storage
     * @param nextNext next after,changing only replica2
     */
    public void notifyServers(String current, String next, String nextNext){
        for (ECSConnection ecsConnection : connections){
            if (next.equals("") && nextNext.equals("") && current.equals(""))
                ecsConnection.reallocate();
            else if(ecsConnection.getHash().equals(next) || ecsConnection.getHash().equals(nextNext))
                ecsConnection.notifyIfDelete(current, next, nextNext);
        }
    }

    /**
     * updateReps method invoked from ecsConnection to send 2 servers with replicas of an updated storage a notification to update them too
     *
     * @param command put/delete to update replicas
     *  @param rep1 hash of a server having the 1 replica
     * @param rep2  hash of a server having the 2 replica
     */
    public void updateReps(String command, String rep1, String rep2){
        for (ECSConnection ecsConnection : connections){
            if(ecsConnection.getHash().equals(rep1) || ecsConnection.getHash().equals(rep2))
                ecsConnection.updateReps(command, rep1, rep2);
        }
    }

    /**
     * removeConnection method removes an ECSConnection from a list, if a server removed
     *
     * @param ip of a connection to remove from a list
     *  @param port of a connection to remove from a list
     */
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

    /**
     * arithmeticHash method increments/decrements a hash value
     *
     * @param hash to change
     * @param increment true if increment, false if decrement
     * @return String of a changed value
     */
    private String arithmeticHash(String hash, boolean increment){
        BigInteger bigHash = new BigInteger(hash, 16);
        bigHash = (increment) ? bigHash.add(BigInteger.ONE) : bigHash.subtract(BigInteger.ONE);
        return bigHash.toString(16);
    }

    /**
     * getMoved method tells if we have to update metadata in servers
     *
     * @return boolean if there was a server reallocation
     */
    public boolean getMoved(){return moved;}

    /**
     * isReachable method checks the availability of the server with the given ip and port
     *
     * @param addr          ip of the server
     * @param openPort      port of the server
     * @param timeOutMillis time out for waiting
     * @return
     */
    private static boolean isReachable(String addr, int openPort, int timeOutMillis) {
        try {
            try (Socket soc = new Socket()) {
                soc.connect(new InetSocketAddress(addr, openPort), timeOutMillis);
            }
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    public Map<String, Metadata> getMetadataMap(){
        return metadataMap;
    }

    public String getNewServer(){return newServer;}

    public String getNextHash(){return nextHash;}

    public String getPrevHash(){return prevHash;}

    public String getNextNextNextHash(){return nextNextNextHash;}

    public String getNextNextHash(){return nextNextHash;}

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
                // NOTA BENE!!!
                //health checks implemented. Only in the end we found one error in getting a wrong ip of a connected server
                // so to avoid any exceptions on this side we decided to comment out this section
                // otherwise it must be working

//                if (!ecs.connections.isEmpty()) {
//                    ecs.connections.stream().forEach(e -> {
//                        if (!isReachable(e.getIP(), e.getPort(), 700)) {
//                            try {
//                                logger.info(" The server in ip : " + e.getIP() + " , port :  " + e.getPort() + " is not responding");
//                                logger.warning(" The server in ip : " + e.getIP() + " , port :  " + e.getPort() + " is not responding");
//                                ecs.removeServer(e.getIP(), e.getPort());
//                            } catch (Exception exception) {
//                                exception.printStackTrace();
//                            }
//                        }
//                    });
//                }

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





