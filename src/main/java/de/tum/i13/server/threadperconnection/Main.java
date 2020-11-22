package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.server.kv.Cache;
import de.tum.i13.server.kv.FIFOLRUCache;
import de.tum.i13.server.kv.KVStoreProcessor;
import de.tum.i13.server.kv.LFUCache;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {

    private static boolean isRunning = true;
    private static Cache cache;
    private static KVStoreProcessor kvStore;

    public static void close() {
        isRunning = false;
    }

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile);
        System.out.println();
        KVStoreProcessor kvStore = new KVStoreProcessor();
        kvStore.setPath(cfg.dataDir);

        if(cfg.cache.equals("FIFO")){
            cache = new FIFOLRUCache(cfg.cacheSize, false);
        }
        else if(cfg.cache.equals("LRU")){
            cache = new FIFOLRUCache(cfg.cacheSize, true);
        }
        else if(cfg.cache.equals("LFU")) {
            cache = new LFUCache(cfg.cacheSize);
        }

        final ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Closing thread per connection kv server");
                try {
                    serverSocket.close();
                    close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        //bind to localhost only
        serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

        //Replace with your Key value server logic.
        // If you use multithreading you need locking
        CommandProcessor logic = new EchoLogic(cache, kvStore);

        while (isRunning) {
            Socket clientSocket = serverSocket.accept();

            //When we accept a connection, we start a new Thread for this connection
            Thread th = new ConnectionHandleThread(logic, clientSocket);
            th.start();
        }
    }
}
