package de.tum.i13.server.kv;

import de.tum.i13.server.threadperconnection.ConnectionHandleThread;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * KVStoreProcessor class to handle the storage file and the cach
 *
 * @author gr9
 */
public class KVStoreProcessor implements KVStore {
    private Path path;
    private File storage;
    private File replica1;
    private File replica2;
    private Scanner scanner;
    private KVMessageProcessor kvmessage;
    private String[] keyvalue;
    private Cache cache;
    private final Logger logger;
    private FileWriter fw;
    private FileWriter fwR1;
    private FileWriter fwR2;

    public void setPath(Path path) {
        this.path = path;
    }

    public void setCache(Cache cache) {
        this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
    }

    public KVStoreProcessor(Path path1) throws IOException {
        this.setPath(path1);
        logger = Logger.getLogger(ConnectionHandleThread.class.getName());
        storage = new File(path + "/storage.txt");
        replica1 = new File(path + "/replica1.txt");
        replica2 = new File(path + "/replica2.txt");
        fw = new FileWriter(storage, false);
        fwR1 = new FileWriter(replica1, false);
        fwR2 = new FileWriter(replica2, false);
    }

    // We have to put the both methods as synchronized because many threads will
    // access them
    // put method adds a key value pair to the cache (local file).

    /**
     * put method adds a key value pair to the cache (local file), if the given kind is storage , otherwise puts it in the given replica
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @param hash  the hash value of the key
     * @param kind  the file where to store , it can be the local repository or replica1 or replica2
     * @return
     * @throws Exception
     */
    @Override
    public synchronized KVMessageProcessor put(String key, String value, String hash, String kind) throws Exception {
        File file;
        FileWriter fWriter;

        boolean added;
        boolean gonethrough = false;
        BigInteger hashToAdd = new BigInteger(hash, 16);
        BigInteger hashToCompare;

        // diff. between replicas or main storage
        if (kind.equals("storage")) {
            file = storage;
            fWriter = fw;
        } else if (kind.equals("replica1")) {
            file = replica1;
            fWriter = fwR1;
        } else {
            file = replica2;
            fWriter = fwR2;
        }

        try {
            if (file.length() == 0) {
                fWriter.write(key + " " + value + " " + hash + "\r\n");
                fWriter.flush();
                kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value);
                this.cache.put(key, value);
            } else {
                scanner = new Scanner(new FileInputStream(file));
                while (scanner.hasNextLine()) {
                    String line;
                    String replacingLine;
                    line = scanner.nextLine();
                    if (line.equals(""))
                        continue;

                    keyvalue = line.split(" ");
                    hashToCompare = new BigInteger(keyvalue[2], 16);
                    if (hashToAdd.compareTo(hashToCompare) <= 0) {

                        if (hashToAdd.equals(hashToCompare)) {
                            replacingLine = (value.equals("null")) ? "" : key + " " + value + " " + hash;
                            added = false;
                        } else {
                            replacingLine = key + " " + value + " " + hash + "\r\n" + line;
                            added = true;
                        }
                        logger.info(replacingLine);
                        Stream<String> lines = Files.lines(Paths.get(path + "/" + kind + ".txt"));
                        String replaced = lines.map(row -> row.replaceAll(line, replacingLine))
                                .collect(Collectors.joining("\r\n"));
                        lines.close();

                        fWriter = new FileWriter(file, false);
                        fWriter.write(replaced + "\r\n");
                        fWriter.flush();
                        this.cache.removeKey(key);

                        if (!value.equals("null")) {
                            kvmessage = (added) ? new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value)
                                    : new KVMessageProcessor(KVMessage.StatusType.PUT_UPDATE, key, value);
                            this.cache.put(key, value);
                        } else {
                            kvmessage = new KVMessageProcessor(KVMessage.StatusType.DELETE_SUCCESS, key, null);
                        }
                        gonethrough = true;
                        break;
                    }
                }
                if (!gonethrough) {
                    String append = key + " " + value + " " + hash + "\r\n";

                    Stream<String> lines = Files.lines(Paths.get(path + "/" + kind + ".txt"));
                    String main = lines.collect(Collectors.joining("\r\n"));
                    lines.close();

                    fWriter = new FileWriter(file, false);
                    fWriter.write(main + "\r\n" + append);
                    fWriter.flush();
                    kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value);
                    this.cache.put(key, value);
                }
                scanner.close();
            }
        } catch (FileNotFoundException fe) {
            logger.warning(fe.getMessage());
            kvmessage = (value.equals("null")) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
                    : new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, value);

        }
        return kvmessage;
    }

    /**
     * get method gets the value of the given key if there is one.
     *
     * @param key given .
     * @return kvMessage for the status of the operation
     * @throws Exception if key not found
     */
    @Override
    public synchronized KVMessageProcessor get(String key) throws Exception {
        kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_ERROR, key, null);

        if (cache.containsKey(key)) {
            String value = cache.get(key);
            kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_SUCCESS, key, value);
        } else {
            Scanner scanner = new Scanner(new FileInputStream(storage));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                keyvalue = line.split(" ");
                if (keyvalue[0].equals(key)) {
                    kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_SUCCESS, keyvalue[0], keyvalue[1]);
                    if (!this.cache.containsKey(key))
                        this.cache.put(key, keyvalue[1]);
                    break;
                }
            }
            scanner.close();
        }
        return kvmessage;
    }

    /**
     * get method for replica gets the value of the given key from the given replica store if there is one
     *
     * @param key given
     * @param rep given from the kvCommandprocessor
     * @return
     * @throws Exception
     */
    public synchronized KVMessageProcessor get(String key, int rep) throws Exception {
        kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_ERROR, key, null);

        if (cache.containsKey(key)) {
            String value = cache.get(key);
            kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_SUCCESS, key, value);
        } else {
            Scanner scanner = null;
            if (rep == 1)
                scanner = new Scanner(new FileInputStream(replica1));
            else if (rep == 2) {
                scanner = new Scanner(new FileInputStream(replica2));
            } else {
                throw new Exception(" wrong replica input ! it should be 1 or 2 .");
            }
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                keyvalue = line.split(" ");
                if (keyvalue[0].equals(key)) {
                    kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_SUCCESS, keyvalue[0], keyvalue[1]);
                    if (!this.cache.containsKey(key))
                        this.cache.put(key, keyvalue[1]);
                    break;
                }
            }
            scanner.close();
        }
        return kvmessage;
    }

    /**
     * getStorage returns a whole storage if removing, part of it by adding a new one
     *
     * @param hash cutting the storage to transfer only one of the parts to another server
     *             if empty, we merge by removing a server and getting the whole storage
     * @return File of a data to transfer
     * @throws IOException
     */
    public File getStorage(String hash) throws IOException {
        File toReturn;
        File toStay;
        if (hash.equals("")) {
            return storage;
        } else {
            BigInteger hashEdge = new BigInteger(hash, 16);
            BigInteger hashToCompare;

            //creating tmp paths
            toStay = new File(path + "/rebalancing1.txt");
            toReturn = new File(path + "/rebalancing2.txt");

            FileWriter fwToReturn = new FileWriter(toReturn, true);
            FileWriter fwToStay = new FileWriter(toStay, true);

            try {
                scanner = new Scanner(new FileInputStream(storage));
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    keyvalue = line.split(" ");
                    hashToCompare = new BigInteger(keyvalue[2], 16);
                    if (hashEdge.compareTo(hashToCompare) > 0) {
                        fwToStay.write(line + "\r\n");
                    } else {
                        fwToReturn.write(line + "\r\n");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            fwToReturn.close();
            fwToStay.close();

            // cut out part we're also leaving for a replica 1
            // replica1 gets to be replica2
            String linesNew = Files.lines(Paths.get(path + "/rebalancing1.txt")).collect(Collectors.joining("\r\n"));
            String linesR2 = Files.lines(Paths.get(path + "/replica1.txt")).collect(Collectors.joining("\r\n"));
            String linesR1 = Files.lines(Paths.get(path + "/rebalancing2.txt")).collect(Collectors.joining("\r\n"));

            fwR1 = new FileWriter(replica1, false);
            fwR2 = new FileWriter(replica2, false);
            fw = new FileWriter(storage, false);

            fwR2.write(linesR2 + "\r\n");
            fwR1.write(linesR1 + "\r\n");
            fw.write(linesNew + "\r\n");

            fwR2.close();
            fwR1.close();
            fw.close();

            toStay.deleteOnExit();
            toReturn.deleteOnExit();

            return toReturn;
        }
    }

    public File getReplica1() {
        return replica1;
    }

    public File getReplica2() {
        return replica2;
    }

    /**
     * removing of replica 1
     *
     * @throws IOException
     */
    public void removeReplica1() throws IOException {
        fwR1 = new FileWriter(replica1, false);
    }

    /**
     * removing of replica 2
     *
     * @throws IOException
     */
    public void removeReplica2() throws IOException {
        fwR2 = new FileWriter(replica2, false);
    }
}
