package de.tum.i13.server.kv;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KVClientStore implements KVStore {
    private String path = "C:\\users\\aina21\\owncdb\\storage.txt";
    private File storage = new File(path);
    private FileOutputStream fileOutputStream;
    private Scanner scanner;
    private KVMessageProcessor kvmessage;
    private String[] keyvalue;
    private boolean updated;

    public KVClientStore(){

    }

    @Override
    public KVMessageProcessor put(String key, String value) throws Exception {
        this.updated = false;
        try {
            if (!storage.exists()) {
                storage.createNewFile();
            }
            scanner = new Scanner(new FileInputStream(storage));
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                keyvalue = line.split(" ");
                if (keyvalue[0].equals(key)) {
                    this.updated = true;
                    Path path1 = Paths.get(path);
                    Stream<String> lines = Files.lines(path1);
                    String replacingLine = (value == null)?"": key + " " + value + "\r\n" ;

                    List<String> replaced = lines.map(row -> row.replaceAll(line, replacingLine)).collect(Collectors.toList());
                    Files.write(path1, replaced);
                    lines.close();
                    kvmessage = (value == null)? new KVMessageProcessor(KVMessage.StatusType.DELETE_SUCCESS, key, null)
                            :new KVMessageProcessor(KVMessage.StatusType.PUT_UPDATE, key, value);
                    break;
                }
                }
            scanner.close();
            if (!updated) {
                String message = key + " " + value + "\r\n";
                byte[] bytesOutput = message.getBytes();
                fileOutputStream = new FileOutputStream(storage);
                fileOutputStream.write(bytesOutput);
                fileOutputStream.flush();
                kvmessage = new KVMessageProcessor(KVMessage.StatusType.PUT_SUCCESS, key, null
                );
            }
        } catch (FileNotFoundException fe) {
            System.out.println(fe);
            kvmessage = (value==null)?new KVMessageProcessor(KVMessage.StatusType.DELETE_ERROR, key, null)
                    :new KVMessageProcessor(KVMessage.StatusType.PUT_ERROR, key, null);
        } finally {
            try {
                fileOutputStream.close();
            } catch (IOException ioe) {
                System.out.println("Error in closing the stream");
            }
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

}
