package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.KVCommandProcessor;

import java.io.*;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * ConnectionHandleThread class that will handle the thread of each client
 *
 * @author gr9
 *
 */
public class InnerConnectionHandleThread extends Thread {

    private final KVCommandProcessor cp;

    private final InetSocketAddress bootstrap;
    private final String hash;
    private final String ip;
    private final int port;
    private boolean shuttingDown;
    private boolean notShutDown;

    public InnerConnectionHandleThread(KVCommandProcessor commandProcessor,
                                       InetSocketAddress bootstrap,
                                       String ip,
                                       int port) throws NoSuchAlgorithmException {
        this.cp = commandProcessor;
        this.bootstrap = bootstrap;
        this.hash = hashMD5(ip + port);
        this.ip = ip;
        this.port = port;
        notShutDown = true;
    }

    public static Logger logger = Logger.getLogger(InnerConnectionHandleThread.class.getName());

    @Override
    /*
     * run() method
     */
    public void run() {
        String nextNeighbour;
        String nextNextNeighbour;
        String prevNeighbour;
        String cutter;

        logger.info(bootstrap.getHostString() + ":" +  bootstrap.getPort());

        // ip, port -> bootstrap, ecs ip, port
        try(Socket socket = new Socket(bootstrap.getHostString(), bootstrap.getPort())){
            BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter outECS = new PrintWriter(socket.getOutputStream());

            logger.info("Started an ECS connection");

            outECS.write("IAmNew" + " " + this.ip + ":" + this.port + "\r\n");
            outECS.flush();

            logger.info("Notified ecs about a new server");
            String line;

            while(notShutDown){
                line = inECS.readLine();

                if(line.equals("NewServer")){

                    cutter = inECS.readLine();  				// newly added server
                    nextNeighbour = inECS.readLine();           // server we have our data at

                    nextNextNeighbour = inECS.readLine();
                    prevNeighbour = inECS.readLine();
                    logger.info("cutter:" + cutter);
                    logger.info("next:" + nextNeighbour);
                    logger.info("nextnext:" + nextNextNeighbour);
                    logger.info("prev:" + prevNeighbour);

                    if(nextNextNeighbour.equals(this.hash)){
                        logger.info("nextnext");
                        this.cp.getKVStore().removeReplica2();
                    }
                    if(nextNeighbour.equals(this.hash)){
                        logger.info("next");

                        // in kvstoreprocessor toReturn should be saved to the fst replica
                        this.transfer(cutter, nextNeighbour);
                        logger.info("Transferring data to a new server");

                        if(!nextNextNeighbour.equals(" ")){
                            this.transferRep1to2(nextNextNeighbour);
                            logger.info("Transferring replica of a new server to a next after next server");
                        }
                    }
                    if(prevNeighbour.equals(this.hash)){
                        logger.info("prev");
                        this.transferStorageRep1(cutter);
                        logger.info("Transferred replicas to a new server");
                    }
                }else if(line.startsWith("metadata") || line.startsWith("firstmetadata")){
                    cp.process(line);
                }else if(line.equals("DeletingAServer")){
                    String current = inECS.readLine();
                    String next = inECS.readLine();
                    String nextNext = inECS.readLine();

                    if(this.hash.equals(nextNext)){
                        this.cp.getKVStore().removeReplica2();
                    }
                    if(this.hash.equals(next)){
                        if(!nextNext.equals(""))
                            this.transferRep2to2(nextNext);
                        this.cp.getKVStore().removeReplica2();
                        this.cp.getKVStore().removeReplica1();
                    }
                    if(hash.equals(current)){
                        if(!next.equals(""))
                            this.transfer(next, "");

                        if(!nextNext.equals(""))
                            this.transferRep12(next);
                        else{
                            this.cp.getKVStore().removeReplica1();
                            this.cp.getKVStore().removeReplica2();
                        }
                    }
                    this.notShutDown = false;
                }else if(line.startsWith("put") || line.startsWith("delete")){
                    String rep1 = inECS.readLine();
                    String rep2 = inECS.readLine();
                    String[] command = line.split(" ");
                    if(this.hash.equals(rep1)){
                        if(command[0].equals("put"))
                            this.cp.getKVStore().put(command[1], command[2], command[3], "replica1");
                        else
                            this.cp.getKVStore().put(command[1], null, command[2], "replica1");
                    }
                    if(this.hash.equals(rep2)){
                        if(command[0].equals("put"))
                            this.cp.getKVStore().put(command[1], command[2], command[3], "replica2");
                        else
                            this.cp.getKVStore().put(command[1], null, command[2], "replica2");
                    }
                }
                if(this.cp.getUpdates()){
                    outECS.write(cp.getToReps().get(0) + ":" + cp.getToReps().get(1) + ":" + cp.getToReps().get(2) + "\r\n");
                    outECS.flush();
                    cp.setUpdateReps(false);
                }
                if(this.shuttingDown){
                    outECS.write("MayIShutDownPlease " + this.ip + ":" + this.port + " " + this.hash + "\r\n");
                    outECS.flush();
                    logger.info("Request to ECS to be allowed to shut down");

                    Thread.yield();
                    if(inECS.readLine().equals("YesYouMay")){
                        logger.info("Got approval. Shutting down in a process");
                    }
                }
            }
            inECS.close();
            outECS.close();
            logger.info("Closed an ECS connection");
        }catch(Exception ie){
            ie.printStackTrace();
        }
    }


    /**
     * transfer method connects with a neighbour server to transfer all storage data if it is shutting down,
     * 	otherwise only the part of a kvstorage to a new server
     * @param newServer server transfer to
     * @param nextServer is our server to transfer from, a neighbour
     */
    private void transfer(String newServer, String nextServer) throws IOException {
        String newIP = cp.getMetadata().get(newServer).getIP();
        int newPort = cp.getMetadata().get(newServer).getPort();

        File storage = (nextServer.equals("")) ? this.cp.getKVStore().getStorage("")
                : this.cp.getKVStore().getStorage(newServer);

        if(storage.length() != 0){
            try(Socket socket = new Socket(newIP, newPort)){
                PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
                Scanner scanner = new Scanner(new FileInputStream(storage));
                while (scanner.hasNextLine()){
                    String transfer = "transferring " + scanner.nextLine();
                    outTransfer.write(transfer);
                }
                outTransfer.write("You'reGoodToGo" + "\r\n");
                outTransfer.flush();
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * transfer2  universal method to send a two files to @server
     * @param server hash value of a server to send files
     */
    private void transfer2(String server, File file1, File file2){
        String newIP = cp.getMetadata().get(server).getIP();
        int newPort = cp.getMetadata().get(server).getPort();

        try (Socket socket = new Socket(newIP, newPort)){
            PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
            Scanner scanner1 = new Scanner(new FileInputStream(file1));
            Scanner scanner2 = new Scanner(new FileInputStream(file2));

            while (scanner1.hasNextLine()){
                outTransfer.write("replica1 " + scanner1.nextLine() + "\r\n");
            }
            while (scanner2.hasNextLine()){
                outTransfer.write("replica2 " + scanner1.nextLine() + "\r\n");
            }
            outTransfer.flush();
            scanner1.close();
            scanner2.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * transferStorageRep1 method to send a storage as a replica and replica1 as replica2 to the next neighbour
     * @param newServer hash value of a server to send a file to (new one)
     */
    private void transferStorageRep1(String newServer) throws IOException {
        File replica1 = this.cp.getKVStore().getStorage("");
        File replica2 = this.cp.getKVStore().getReplica1();
        this.transfer2(newServer, replica1, replica2);
    }


    /**
     * transferRep12 method to send both replicas of the same order to @server
     * @param server hash value of a server to send replicas to
     */
    private void transferRep12(String server) throws IOException {
        File replica1 = this.cp.getKVStore().getReplica1();
        File replica2 = this.cp.getKVStore().getReplica2();
        this.transfer2(server, replica1, replica2);
    }

    /**
     * transferOne method to send one file to @server
     * @param server hash value of a server to send a file to
     */
    private void transferOne(String server, File file){
        String nextNextIP = cp.getMetadata().get(server).getIP();
        int nextNextPort = cp.getMetadata().get(server).getPort();

        try (Socket socket = new Socket(nextNextIP, nextNextPort)){
            PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
            Scanner scanner = new Scanner(new FileInputStream(file));

            while (scanner.hasNextLine()){
                outTransfer.write("replica2 " + scanner.nextLine() + "\r\n");
            }
            outTransfer.flush();
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void transferRep1to2(String nextNextServer){
        File replica2 = this.cp.getKVStore().getReplica1();
        this.transferOne(nextNextServer, replica2);
    }

    private void transferRep2to2(String server){
        File replica2 = this.cp.getKVStore().getReplica2();
        this.transferOne(server, replica2);
    }

    /**
     * hashMD5 method hashes a given key to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    private String hashMD5(String key) throws NoSuchAlgorithmException {
        MessageDigest msg = MessageDigest.getInstance("MD5");
        byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));
        String myHash = new BigInteger(1, digested).toString(16);

        return myHash;
    }

    public void setShuttingDown(boolean shuttingDown){
        this.shuttingDown = shuttingDown;
    }

    public boolean getShutDown(){
        return this.notShutDown;
    }

}
