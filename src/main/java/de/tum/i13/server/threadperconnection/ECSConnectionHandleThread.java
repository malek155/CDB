package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.kv.KVCommandProcessor;

import java.io.*;
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
public class ECSConnectionHandleThread extends Thread {

    private final KVCommandProcessor cp;

    private final InetSocketAddress bootstrap;
    private final String hash;
    private final String ip;
    private final int port;
    private ConnectionHandleThread client;

    public ECSConnectionHandleThread(KVCommandProcessor commandProcessor,
                                  InetSocketAddress bootstrap,
                                  String ip,
                                  int port, ConnectionHandleThread client) throws NoSuchAlgorithmException {
        this.cp = commandProcessor;
        this.bootstrap = bootstrap;
        this.hash = hashMD5(ip + port);
        this.ip = ip;
        this.port = port;
        this.client = client;
    }

    public static Logger logger = Logger.getLogger(ECSConnectionHandleThread.class.getName());

    @Override
    /*
     * run() method
     */
    public void run() {

        boolean notShutDown = true;
        String neighbour;
        String cutter;

        // ip, port -> bootstrap, ecs ip, port
        try(Socket socket = new Socket(bootstrap.getHostString(), bootstrap.getPort())){
            BufferedReader inECS = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter outECS = new PrintWriter(socket.getOutputStream());

            logger.info("Started an ECS connection");

            while(notShutDown){
                if(inECS.readLine().equals("NewServer")){
                    cutter = inECS.readLine();  				// newly added server
                    neighbour = inECS.readLine();			// server we have our data at
                    if(neighbour.equals(this.hash)){
                        this.transfer(cutter, neighbour);
                        logger.info("Transferred data to a new server");
                    }
                }
                if(client.getShuttingDown()){
                    outECS.write("MayIShutDownPlease " + this.ip + ":" + this.port + " " + this.hash + "\r\n");
                    outECS.flush();
                    logger.info("Request to ECS to shut down");

                    Thread.yield();
                    if(inECS.readLine().equals("YesYouMay")){
                        neighbour = inECS.readLine();
                        this.transfer(neighbour, "");
                        this.client.setClosing(true);
                        notShutDown = false;
                        logger.info("Shutting down in a process");
                    }
                }
            }
            inECS.close();
            outECS.close();
            logger.info("Closed an ECS connection");
        }catch(IOException ie){
            ie.printStackTrace();
        }
    }


    /**
     * transfer method connects with a neighbour server to transfer all data if it is shutting down,
     * 	otherwise only the part of a kvstorage to a new server
     * @param transferTo server transfer to
     * @param ours is our server to transfer from, a neigbour
     */
    private void transfer(String transferTo, String ours){
        String newIP = cp.getMetadata().get(transferTo).getIP();
        int newPort = cp.getMetadata().get(transferTo).getPort();

        try (Socket socket = new Socket(newIP, newPort)){
            File storage = (ours.equals("")) ? this.cp.getKVStore().getStorage("")
                    : this.cp.getKVStore().getStorage(transferTo);
            PrintWriter outTransfer = new PrintWriter(socket.getOutputStream());
            Scanner scanner = new Scanner(new FileInputStream(storage));

            while (scanner.hasNextLine()) {
                outTransfer.write("transferring " + scanner.nextLine() + "\r\n");
                outTransfer.flush();
            }
            if(ours.equals("")){
                outTransfer.write("You'reGoodToGo\r\n");
                outTransfer.flush();
            }
            scanner.close();
            outTransfer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * hashMD5 method hashes a given key to its Hexadecimal value with md5
     *
     * @return String of hashvalue in Hexadecimal
     */
    private String hashMD5(String key) throws NoSuchAlgorithmException {

        MessageDigest msg = MessageDigest.getInstance("MD5");
        byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));

        return new String(digested);
    }

}
