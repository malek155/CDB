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
    private String path = "storage.txt";
    private File storage = new File(path);
    private FileOutputStream fileOutputStream;
    private Scanner scanner;
    private KVMessageProcessor kvmessage;
    private String[] keyvalue;
    private boolean change;
    private Cache cache;


    public KVStoreProcessor() {
    }


    @Override
    public KVMessageProcessor put(String key, String value) throws Exception {
        this.change = false;
        try {
            if (!storage.exists())
                storage.createNewFile();

            scanner = new Scanner(new FileInputStream(storage));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                keyvalue = line.split(" ");
                if (keyvalue[0].equals(key)) {
                    this.change = true;
                    Path path1 = Paths.get(path);
                    Stream<String> lines = Files.lines(path1);
                    String replacingLine = (value == null) ? "" : key + " " + value + "\r\n";

                    List<String> replaced = lines.map(row -> row.replaceAll(line, replacingLine)).collect(Collectors.toList());
                    Files.write(path1, replaced);
                    lines.close();
                    kvmessage = (value == null) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_SUCCESS, key, null)
                            : new KVMessageProcessor(KVMessage.StatusType.PUT_UPDATE, key, value);

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
                kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, null
                );
            }
        } catch (FileNotFoundException fe) {
            System.out.println(fe);
            kvmessage = (value == null) ? new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
                    : new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, null);
        }
        return kvmessage;
    }

    @Override
    public KVMessageProcessor get(String key) throws Exception {
        kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_ERROR, null, null);
        Scanner scanner = new Scanner(new FileInputStream(storage));
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            keyvalue = line.split(" ");
            if (keyvalue[0].equals(key)) {
                kvmessage = new KVMessageProcessor(KVMessage.StatusType.GET_SUCCESS, keyvalue[0], keyvalue[1]);
                break;
            }
        }
        scanner.close();
        return kvmessage;
    }

    //added method to get the used Cache
    public Cache getCache() {
        return this.cache;
    }

    public static void main(String[] args) throws Exception {
        KVStoreProcessor kvStoreProcessor = new KVStoreProcessor();
        kvStoreProcessor.put("key0", "value0");
        System.out.println(kvStoreProcessor.get("key0").getValue());
        kvStoreProcessor.put("key1", "value1");
        kvStoreProcessor.put("key2", "value3");
        kvStoreProcessor.put("key1", "value3");
        kvStoreProcessor.put("key0", null);
        System.out.println(kvStoreProcessor.put("key1", "value4").getStatus() + kvStoreProcessor.get("key1").getValue());
    }

}