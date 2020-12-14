package de.tum.i13.server.threadperconnection;

import com.sun.tools.jdeprscan.scan.Scan;
import de.tum.i13.server.kv.*;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Scanner;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {
    // used to shut down the server , maybe we need it
    private static boolean isRunning = true;
    private static Cache cache;
    private KVStoreProcessor kvStore;
    public static String start;
    public static String end;
    private Map<String, Metadata> metadata;
    private static boolean shuttingDown = false;
    private static boolean shutDown = false;
    private static String nextIP;
    private static int nextPort;
    private static File storage;

    public Main nextServer;

    public Main(Cache cache, String start, String end) throws IOException {
        if (cache.getClass().equals(FIFOLRUCache.class)) {
            cache = (FIFOLRUCache) cache;
        } else if (cache.getClass().equals(LFUCache.class)) {
            cache = (LFUCache) cache;
        }
        this.start = start;
        this.end = end;

    }

    public String getNextIP() {
        return metadata.get(nextServer).getIP();
    }

    public void findNextIP() {
        this.nextIP = metadata.get(nextServer).getIP();
    }

    public void findNextPort() {
        this.nextPort = metadata.get(nextServer).getPort();
    }

    public void setStart(String newstart) {
        start = newstart;
    }

    public void setEnd(String newend) {
        end = newend;
    }

    public void setStorage() {
        storage = kvStore.getStorage();
    }

    public void setMetadata(Map<String, Metadata> metadata) {
        this.metadata = metadata;
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    static ServerSocket serverSocket = null;

    static {
        try {
            serverSocket = new ServerSocket();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * main() method where our serversocket will be initialized
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args); // Do not change this
        setupLogging(cfg.logfile);
        KVStoreProcessor kvStore = new KVStoreProcessor();
        kvStore.setPath(cfg.dataDir);

        // now you can connect to ecs
        try (Socket socket = new Socket(cfg.bootstrap.getAddress(), cfg.bootstrap.getPort())) {
            BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter outECS = new PrintWriter(socket.getOutputStream());
            while (!shutDown) {
                if (shuttingDown) {
                    outECS.write("mayishutdownplz " + end + "\r\n");
                    outECS.flush();
                    if (inECS.readLine().equals("yesyoumay")) {
                        transfer();
                        outECS.write("transferred" + "\r\n");
                        outECS.flush();
                        shutDown = true;
                    }
                }
            }
            inECS.close();
            outECS.close();
        } catch (IOException ie) {
            ie.printStackTrace();
        }

        // now we can open a listening serversocket
// changed declaration of server socket outside main method
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shuttingDown = true;
                try {
                    if (shutDown)
                        serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        // binding to the server
        serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

        KVCommandProcessor logic = new KVCommandProcessor(kvStore, cache);

        while (isRunning) {
            // Waiting for client to connect
            Socket clientSocket = serverSocket.accept();

            // When we accept a connection, we start a new Thread for this connection
            Thread th = new ConnectionHandleThread(logic, clientSocket);
            th.start();
        }
    }

    //transfer method in ConnectionHandleThread

    public static void transfer() {
        try (Socket socket = new Socket(nextIP, nextPort)) {
            PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
            //read through storage
            Scanner scanner = new Scanner(new FileInputStream(storage));

            while (scanner.hasNextLine()) {
                //String line = scanner.nextLine();
                outTransfer.write("transferring " + scanner.nextLine() + "\r\n");
                outTransfer.flush();
            }
            scanner.close();
            outTransfer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //mergeFiles method to add two files, needed when a server is being removed
    //file1 is the storage of the server to be removed
    //file2 is the storage of the server that will have his files merged = his path is the destination
    private File mergeFiles(File file1, File file2) throws IOException {
        File mergedFile = null;
        //not gonna use scanner i think
        //Scanner scan1 = new Scanner(file1);
        //Scanner scan2 = new Scanner(file2);
        //are we even gonna keep nextIP and nextPort in Main ?
        Socket destSocket = new Socket(nextIP, nextPort);
mergedFile
        //pw_mergedFile to write in the file of destination
        PrintWriter pw_mergedFile = new PrintWriter(destSocket.getOutputStream());
        BufferedReader br1 = new BufferedReader(new FileReader(storage));//which storage???
        String line1 = br1.readLine();

        //putting each line of file1 into des
        while (line1 != null) {
            pw_mergedFile.println(line1);
            line1 = br1.readLine();
        }
        br1.close();
//buff for second file
        BufferedReader br2 = new BufferedReader(new FileReader(storage));//which storage??
        String line2 = br2.readLine();
        while (line2 != null) {
            pw_mergedFile.println(line2);
            line2 = br2.readLine();
        }
        pw_mergedFile.flush();
        br2.close();
        pw_mergedFile.close();
//maybe we can assign a new path to the merged file or in the same file ?
        return mergedFile;
    }

    //to update the storage of particular server
    //public void setStorage(File newFile) {
    // this.kvStore. = newFile;
    //}


}