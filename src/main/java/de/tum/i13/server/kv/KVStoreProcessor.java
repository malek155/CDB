package de.tum.i13.server.kv;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KVStoreProcessor implements KVStore {
    private Path path;
    private File storage = new File(String.valueOf(path));
    private FileOutputStream fileOutputStream;
    private Scanner scanner;
    private KVMessageProcessor kvmessage;
    private String[] keyvalue;
    private boolean change;
    private Cache cache;

    public void setPath(Path path) {
        this.path = path;
    }

    public void setCache(Cache cache) {
        this.cache = (cache.getClass().equals(LFUCache.class)) ? (LFUCache) cache : (FIFOLRUCache) cache;
    }

    public KVStoreProcessor() {
    }

    // We have to put the both methods as synchronized because many threads will
    // access them
    @Override
    public synchronized KVMessageProcessor put(String key, String value) throws Exception {
        this.change = false;
        try {

            scanner = new Scanner(new FileInputStream(storage));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                keyvalue = line.split(" ");
                if (keyvalue[0].equals(key)) {
                    this.change = true;
                    Path path1 = Paths.get(String.valueOf(path));
                    Stream<String> lines = Files.lines(path1);
                    String replacingLine = (value == null) ? "" : key + " " + value + "\r\n";

                    List<String> replaced = lines.map(row -> row.replaceAll(line, replacingLine))
                            .collect(Collectors.toList());
                    Files.write(path1, replaced);
                    lines.close();
                    this.cache.removeKey(key);
                    if (value != null) {
                        this.cache.put(key, value);
                        kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_UPDATE, key, value);
                    } else {
                        // already deleted in cache
                        kvmessage = new KVMessageProcessor(KVMessage.StatusType.DELETE_SUCCESS, key, null);
                    }

                    break;
                }
            }
            scanner.close();
            if (this.change == false) {
                String message = key + " " + value + "\r\n";
                byte[] bytesOutput = message.getBytes();
                FileWriter fileWriter = new FileWriter(storage.getName(), true);
                BufferedWriter bw = new BufferedWriter(fileWriter);
                bw.write(message);
                bw.close();
                fileWriter.close();
                kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, value);
                this.cache.put(key, value);
            }
        } catch (FileNotFoundException fe) {
            System.out.println(fe);
            kvmessage = (value == null) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
                    : new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, null);
        }
        return kvmessage;
    }

    @Override
    public synchronized KVMessageProcessor get(String key) throws Exception {
        // we need the key for the response
        kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_ERROR, key, null);
        Scanner scanner = new Scanner(new FileInputStream(storage));
        // we can use the streams also here
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
        return kvmessage;
    }

    public static void main(String[] args) throws Exception {
        KVStoreProcessor kvStoreProcessor = new KVStoreProcessor();
        kvStoreProcessor.put("key0", "value0");
        System.out.println(kvStoreProcessor.get("key0").getValue());
        kvStoreProcessor.put("key1", "value1");
        kvStoreProcessor.put("key2", "value3");
        kvStoreProcessor.put("key1", "value3");
        kvStoreProcessor.put("key0", null);
        System.out
                .println(kvStoreProcessor.put("key1", "value4").getStatus() + kvStoreProcessor.get("key1").getValue());
    }

}