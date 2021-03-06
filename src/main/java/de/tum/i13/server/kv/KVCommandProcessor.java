package de.tum.i13.server.kv;

import de.tum.i13.server.kv.KVMessage.StatusType;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Metadata;
import de.tum.i13.shared.MetadataReplica;

import java.io.IOException;
import java.math.BigInteger;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * KVCommandProcessor to handle the client requests that contains put or get
 * requests
 *
 * @author gr9
 */
public class KVCommandProcessor implements CommandProcessor {
    // we forward the lines that have put , get , delete from the Echologic to this
    // class because it is responsible to interact with the KVStore and handle those
    // commands
    private KVStoreProcessor kvStore;

    // instance of metadata
    private static TreeMap<String, Metadata> metadata;
    private TreeMap<String, MetadataReplica> metadata2;
    // start and end (for now I suppose that I am able to get them from the main)
    private String start;
    private String end;
    private String hash;
    // static boolean variable for read only
    private boolean readOnly;
    // boolean variable to know if the server is initiated from the ECS
    // volatile keyword because this variable is expected to be changed from another
    // thread
    private volatile boolean initiated;
    // boolean for knowing when to update replicas of a storage
    private boolean updateReps = false;
    // toReps for info for replicas
    private ArrayList<String> toReps = new ArrayList<>();

    private String ip;
    private int port;
    private int retention;
    private static Broker broker;


    public KVCommandProcessor() {
    }

    public static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    // new constructor having the metadata instance and start end of the range
    public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache, String ip,
                              int port, int retention) throws NoSuchAlgorithmException {
        this.kvStore = kvStore;
        kvStore.setCache(cache);
        this.hash = this.hashMD5(ip + port);
        this.end = this.hash;
        this.initiated = false;
        this.readOnly = true;
        logger.info("New thread for server started, initializing");
        this.ip = ip;
        this.port = port;
        this.retention = retention;
        if (broker == null)
            broker = new Broker(kvStore.getPath(), retention);
    }

    public KVCommandProcessor(KVStoreProcessor kvStore, Cache cache, String ip,
                              int port) throws NoSuchAlgorithmException {
        this.kvStore = kvStore;
        kvStore.setCache(cache);
        this.hash = this.hashMD5(ip + port);
        this.end = this.hash;
        this.initiated = false;
        this.readOnly = true;
        logger.info("New thread for server started, initializing");
        this.ip = ip;
        this.port = port;
        this.retention = 40;
        if (broker == null)
            broker = new Broker(kvStore.getPath(),retention);
    }

    /**
     * process method that handles the requests
     *
     * @param command - command got from a client or another server
     * @return answer after processing
     */
    @Override
    public String process(String command) throws Exception {
        logger.info("received command: " + command.trim());
        String[] input = command.split(" ");

        String reply = command;


        if ((input[0].equals("put") || input[0].equals("get") || input[0].equals("delete")
                || input[0].equals("publish") || input[0].equals("subscribe") || input[0].equals("unsubscribe")) && input.length != 1 && initiated) {

            this.start = metadata.get(hash).getStart();
            logger.info("nice: " + start + " " + end + " " + hash);

            if ((!(input[0].equals("subscribe") || input[0].equals("unsubscribe")) && isInTheRange(this.hashMD5(input[1]), start, end))
                    || (input[0].equals("subscribe") || input[0].equals("unsubscribe")) && isInTheRange(this.hashMD5(input[2]), start, end)) {
                KVMessage msg;
                String response = "";

                try {

                    if (readOnly && (input[0].equals("put") || input[0].equals("delete") || input[0].equals("publish"))) {
                        logger.info("Server is under rebalancing, only getting keys is available");
                        response = "server_write_lock";
                    }
                    if (!readOnly && (input[0].equals("put") || input[0].equals("delete") || input[0].equals("publish"))) {
                        if (input.length != 3 && input[0].equals("put")) {
                            logger.warning("not a suitable command for putting keys-values!");
                            response = "not a suitable command for putting keys-values!";
                            throw new IOException("Put Request needs a key and a value !");
                        } else if (input.length != 2 && input[0].equals("delete")) {
                            logger.warning("not a suitable command for deleting keys-values!");
                            response = "not a suitable command for deleting keys-values!";
                            throw new IOException("Delete Request needs only a key !");
                        } else if (input.length != 3 && input[0].equals("publish")) {
                            logger.warning("not a suitable command for publishing keys-values!");
                            response = "not a suitable command for publishing keys-values!";
                            throw new IOException("Publish Request needs a key and value!");
                        }
                        if (input[0].equals("put"))
                            msg = this.kvStore.put(input[1], input[2], hashMD5(input[1]), "storage");
                        else if (input[0].equals("publish"))
                            msg = this.kvStore.publish(input[1], input[2], hashMD5(input[1]), "storage");
                        else
                            msg = this.kvStore.put(input[1], "null", hashMD5(input[1]), "storage");

                        logger.info("status:" + msg.getStatus().toString());
                        if (msg.getStatus().equals(StatusType.PUT_ERROR) || msg.getStatus().equals(StatusType.DELETE_ERROR) || msg.getStatus().equals(StatusType.PUBLICATION_ERROR)) {
                            logger.info("Error occurred by putting/deleting/publishing a value ");
                            String value;
                            if (!input[0].equals("put")) // if delete
                                value = "Error occurred. Try again later.";
                            else
                                value = msg.getValue();
                            response = msg.getStatus().toString() + " " + msg.getKey() + " " + value;
                        } else {
                            String value;
                            if (input[0].equals("publish")) {
                                value = msg.getValue();
                                this.broker.notify(msg.getKey(), value);
                            }    // value not empty if success
                            else
                                value = "";
                            response = msg.getStatus().toString() + " " + msg.getKey() + " " + value;
                            if (metadata.size() > 2) {
                                // 1: command with a hash - put/delete/publish <...> , 2: replica1, 3:rep2
                                toReps.add(command + " " + hashMD5(input[1]));
                                toReps.add(this.metadata2.get(hash).getEndRep1());
                                toReps.add(this.metadata2.get(hash).getEndRep2());
                                this.updateReps = true;
                            }
                        }
                    } else if (input[0].equals("get")) {
                        if (input.length != 2) {
                            response = "not a suitable command for getting values!";
                            logger.warning("not a suitable command for getting values!");
                            throw new Exception("Get Request needs only a key !");
                        }
                        msg = this.kvStore.get(input[1]);
                        if (msg.getStatus().equals(StatusType.GET_ERROR)) {
                            logger.info("Error occurred by getting a value ");
                            response = msg.getStatus().toString() + " " + msg.getKey() + ": Key not found.";
                        } else {
                            logger.info("Got a value");
                            response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
                        }
                    } else if (input[0].equals("subscribe")) { // subscribe sid, key, port, ip
                        broker.subscribe(input);
                    } else if (input[0].equals("unsubscribe")) {// sid, key, ip, port
                        broker.unsubscribe(input[1], input[2]);
                    }

                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
                reply = response;
            } else if (input[0].equals("get") || input[0].equals("subscribe") || input[0].equals("unsubscribe")) {
                logger.info("Checking the replicas of the server for get/subscribing/unsubscribing request ");
                KVMessage msg;
                String response = "";
                try {
                    if (input.length != 2 && input[0].equals("get")) {
                        response = "not a suitable command for getting values!";
                        logger.warning("not a suitable command for getting values!");
                        throw new Exception("Get Request needs only a key !");
                    } else if (input.length != 3 && (input[0].equals("subscribe") || input[0].equals("unsubscribe"))) {
                        response = "not a suitable command for un/subscribing!";
                        logger.warning("not a suitable command for un/subscribing!");
                        throw new Exception("Un/Subscribe Request needs a key and an ID!");
                    }
                    if (input[0].equals("get") && isInTheRange(this.hashMD5(input[1]), metadata2.get(hash).getStartRep1(), end)) {
                        if ("get".equals(input[0])) {
                            msg = this.kvStore.get(input[1], 1);
                            if (msg.getStatus().equals(StatusType.GET_ERROR)) {
                                logger.info("Error occured by getting a value from replica 1 ");
                                response = msg.getStatus().toString() + " " + msg.getKey() + ": Key not found.";
                            } else {
                                logger.info("Got a value from replica 1");
                                response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
                            }
                        }
                    } else if (input[0].equals("get") && isInTheRange(this.hashMD5(input[1]), metadata2.get(hash).getStartRep2(), end)
                            || (input[0].equals("subscribe") || input[0].equals("unsubscribe")) && isInTheRange(this.hashMD5(input[2]), metadata2.get(hash).getStartRep2(), end)) {
                        switch (input[0]) {
                            case "get":
                                msg = this.kvStore.get(input[1], 2);
                                if (msg.getStatus().equals(StatusType.GET_ERROR)) {
                                    logger.info("Error occured by getting a value from replica 2 ");
                                    response = msg.getStatus().toString() + " " + msg.getKey() + ": Key not found.";
                                } else {
                                    logger.info("Got a value from replica 2");
                                    response = msg.getStatus().toString() + " " + msg.getKey() + " " + msg.getValue();
                                }
                                break;
                            case "subscribe":
                                broker.subscribe(input);
                                response = "subscribe_success " + input[1] + " " + input[2];
                                break;
                            case "unsubscribe":
                                broker.unsubscribe(input[1], input[2]);
                                response = "unsubscribe_success " + input[1];
                                break;
                        }
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

                reply = response;

            } else {
                logger.info("Server is not responsible for a key");
                if (this.initiated) {
                    String meta = metadata.keySet().stream()
                            .map(key -> metadata.get(key).getStart() + ","
                                    + key + ","
                                    + metadata.get(key).getIP() + ":"
                                    + metadata.get(key).getPort())
                            .collect(Collectors.joining(";"));

                    reply = "server_not_responsible\r\n" + meta;
                } else
                    reply = "server_stopped";
            }
        } else if ((input[0].equals("put") || input[0].equals("get") || input[0].equals("delete") || input[0].equals("publish")
                || input[0].equals("subscribe") || input[0].equals("unsubscribe")) && input.length == 1 && initiated) {
            reply = "not a suitable command";
        } else if (input[0].equals("You'reGoodToGo")) {
            this.initiated = true;
            readOnly = false;
            if (metadata != null) {
                this.start = metadata.get(hash).getStart();
                this.end = metadata.get(hash).getEnd();
            }
            logger.info("Server is ready");
            this.readOnly = false;
        } else if (input[0].equals("replica1")) {
            // 1 = key, 2 = value, 3 = hash
            kvStore.put(input[1], input[2], input[3], "replica1");
            logger.info("Updating replica1");
        } else if (input[0].equals("replica2")) {
            kvStore.put(input[1], input[2], input[3], "replica2");
            logger.info("Updating replica2");
        } else if (input[0].equals("transferring")) {
            // 1: key, 2: value; 3: hash, 4: kind of file
            this.kvStore.put(input[1], input[2], input[3], "storage");
            logger.info("Putting a new kv-pair, transferred from other servers");
        } else if (input[0].equals("metadata")) {
            String[] entry = command.split("=");
            String hashLokal = entry[0].split(" ")[1];
            String[] metadatanew = entry[1].split(" ");

            // hash: key; 1 entry: ip, 2 entry: port, 3 entry: start, 4: end
            metadata.put(hashLokal, new Metadata(metadatanew[0], Integer.parseInt(metadatanew[1]), metadatanew[2], metadatanew[3]));

            if (metadatanew.length == 5) {
                metadata2 = metadataMap2();
                logger.info("restructuring of metadata2");
                logger.info("Updated metadata from ECS");
            }
        } else if (input[0].equals("firstmetadata")) {
            if (metadata == null) {
                metadata = new TreeMap<>();
                this.initiated = true;
                this.readOnly = false;
            } else {
                metadata.clear();
            }
            String[] entry = command.split("=");
            String hashLocal = entry[0].split(" ")[1];
            String[] metadatanew = entry[1].split(" ");

            // hash: key; 1 entry: ip, 2 entry: port, 3 entry: start, 4: end
            metadata.put(hashLocal, new Metadata(metadatanew[0], Integer.parseInt(metadatanew[1]), metadatanew[2], metadatanew[3]));

            if (metadatanew.length == 5) {
                metadata2 = metadataMap2();
                logger.info("restructuring of metadata2");
                logger.info("Updated metadata from ECS");
            }
        } else if (input[0].equals("keyrange")) {
            if (this.initiated) {
                reply = "keyrange_success " + metadata.keySet().stream()
                        .map(key -> metadata.get(key).getStart() + ","
                                + key + ","
                                + metadata.get(key).getIP() + ":"
                                + metadata.get(key).getPort())
                        .collect(Collectors.joining(";"));
                logger.info("Updating metadata on the client side, sending");
            } else
                reply = "server_stopped";
        } else if (input[0].equals("keyrange_read")) {
            if (this.initiated) {
                reply = "keyrange_read_success " + metadata2.keySet().stream()
                        .map(key -> metadata2.get(key).getStartRep2() + "," +
                                key + "," + metadata2.get(key).getIP() + ":"
                                + metadata2.get(key).getPort())
                        .collect(Collectors.joining(";"));

                logger.info("Updating keyranges for a client to read");
                logger.info(metadata2.toString());
            } else
                reply = "server_stopped";
        } else {
            logger.info(String.valueOf(initiated));
            reply = "error: wrong command, please try again!";
            logger.warning("Wrong input from a client");
        }
        if (readOnly && reply.length() == 0) {
            reply = "the server is read only at the moment and can not handle any put request please try later ";
            logger.info("Server is under rebalancing its storage right now, readonly");
        }
        return reply;
    }

    /**
     * isInTheRange Method that takes the key sent from the client and verify
     * whether this key in within the range of this KVServer
     *
     * @param key   given from the client
     * @param start start value of hash
     * @param end   end value of hash
     * @return a boolean saying if the KVServers range contains this key
     */
    private boolean isInTheRange(String key, String start, String end) {
        boolean result = false;

        BigInteger intKey = new BigInteger(key, 16);
        BigInteger intStart = new BigInteger(start, 16);
        BigInteger intEnd = new BigInteger(end, 16);

        // where the start < end
        if (intStart.compareTo(intEnd) < 0) {
            if (intKey.compareTo(intStart) >= 0 && intKey.compareTo(intEnd) <= 0)
                result = true;
        } else {
            if (intKey.compareTo(intStart) >= 0 || intKey.compareTo(intEnd) <= 0)
                result = true;
        }
        logger.info(result + " " + start + " " + end);

        return result;
    }

    public KVStoreProcessor getKVStore() {
        return this.kvStore;
    }

    public Map<String, Metadata> getMetadata() {
        return metadata;
    }

    /**
     * getUpdates a methode to check, if we have to update replicas
     *
     * @return true, if replicas need tobe updated
     */
    public boolean getUpdates() {
        return this.updateReps;
    }

    /**
     * setUpdateReps methode mostly sets updateReps false after updating replicas
     *
     * @param updating: false if updated replicas after putting/deleting keyvalues in a main storage
     */
    public void setUpdateReps(boolean updating) {
        updateReps = updating;
    }

    /**
     * getToReps a methode to check, if we have to update replicas
     *
     * @return a list of command (put/delete), hash of a key, hash of a replica1, hash of a replica2
     */
    public ArrayList<String> getToReps() {
        return this.toReps;
    }

    public void clearToReps() {
        this.toReps = new ArrayList<>();
    }

    public String hashMD5(String key) throws NoSuchAlgorithmException {
        MessageDigest msg = MessageDigest.getInstance("MD5");
        byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));
        String myHash = new BigInteger(1, digested).toString(16);

        return myHash;
    }

    /**
     * metadataMap2 takes the TreeMap of metadata and generates a TreeMap of metadata2 which contains replicas
     *
     * @return metadata of a main range, ranges of replicas
     */
    private TreeMap<String, MetadataReplica> metadataMap2() {
        // I need it to get the replicas
        TreeMap<String, MetadataReplica> metadataMap2 = new TreeMap();
        TreeMap<String, Metadata> meta2 = this.metadata;
        this.metadata.keySet().forEach(key -> {
            Metadata meta1 = metadata.get(key);
            MetadataReplica mdr = new MetadataReplica(meta1.getIP(), meta1.getPort(), meta1.getStart(), meta1.getEnd(), null, null);
            // we get the hash of previous server with the start of this server from the metadata

            String start1 = meta1.getStart();
            BigInteger bighash = new BigInteger(start1, 16);
            bighash = bighash.subtract(BigInteger.ONE);
            String lastHash = bighash.toString(16);

            String start2 = metadata.get(lastHash).getStart();
            mdr.setEndRep1(lastHash);
            mdr.setStartRep1(start2);

            BigInteger bighash2 = new BigInteger(start2, 16);
            bighash2 = bighash2.subtract(BigInteger.ONE);
            String lastLastHash = bighash2.toString(16);

            mdr.setEndRep2(lastLastHash);
            mdr.setStartRep2(metadata.get(lastLastHash).getStart());

            metadataMap2.put(key, mdr);
        });

        return metadataMap2;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());
        return "Connection to MSRG Echo server established: " + address.toString() + "\r\n";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }
}