package de.tum.i13.server.ecs;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.Metadata;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ECSConnection implements Runnable {
    private Socket clientSocket;
    private ECS bigECS;
    private BufferedReader in;
    private PrintWriter out;
    private String ip;
    private int port;
    private String hash;

    public static Logger logger = Logger.getLogger(ECSConnection.class.getName());

    public ECSConnection(Socket clientSocket, ECS bigECS) throws IOException, NoSuchAlgorithmException {
        this.clientSocket = clientSocket;
        this.bigECS = bigECS;
        in = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
        out = new PrintWriter(
                new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));
        ip = clientSocket.getInetAddress().getLocalHost().getHostAddress();
        port = clientSocket.getLocalPort();
        hash = hashMD5(ip+port);
    }

    @Override
    public void run(){
        try {
            logger.info("Started the ECS connection");

            String line;
            while (!clientSocket.isClosed()){
                line = in.readLine();
                String message = this.process(line);
                Thread.yield();
                if (!message.equals("")) {
                    out.write(message);
                    out.flush();
                }
            }
            logger.info("Closing the ECS connection");
            if (out != null)
                out.close();
            if (in != null) {
                in.close();
                clientSocket.close();
            }
        } catch (Exception ie) {
            ie.printStackTrace();
        }
    }

    /**
     * @param line is a command got from servers to process
     * @return String, a corresponding answer to servers
     */
    private String process(String line) throws Exception {
        logger.info("processing");
        String reply = "";
        String[] ipport;
        String[] lines = line.split(" ");
        if (lines[0].equals("MayIShutDownPlease")){
            ipport = lines[1].split(":");
            this.bigECS.shuttingDown(ipport[0], Integer.parseInt(ipport[1]), lines[2]);
            reply = "YesYouMay\r\n";
        }else if (lines[0].equals("IAmNew")){
            ipport = lines[1].split(":");
            if(!bigECS.isAdded(ipport[0], Integer.parseInt(ipport[1]))){
                bigECS.addServer(ipport[0], Integer.parseInt(ipport[1]));
            }
        }else if(lines[0].equals("delete") || lines[0].equals("put")){
            String[] updates = line.split(":");
            bigECS.updateReps(updates[0], updates[1], updates[2]);
        }
        return reply;
    }

    /**
     * sendMeta is a method to send updated metadata  to one server. Invoked from ECS
     */
    public void sendMeta(){
        Map<String, Metadata> map = bigECS.getMetadataMap();
        String metadata = map.keySet().stream()
                .map(key -> "metadata " + key + "=" + map.get(key).toString())
                .collect(Collectors.joining("\r\n"));
        out.write("first"+ metadata + " last" + "\r\n");
        out.flush();
    }

    /**
     * reallocate is a method to send a notification to a server about a newly added server to reallocate data. Invoked from ECS
     */
    public void reallocate(){
        out.write("NewServer\r\n" + bigECS.getNewServer() + "\r\n");
        if(bigECS.getServerRepository().size()>1){
            out.write(bigECS.getNextHash() + "\r\n");
            if(bigECS.getServerRepository().size()>2)
                out.write(bigECS.getNextNextHash() + "\r\n" + bigECS.getPrevHash() + "\r\n");
            else
                out.write(" \r\n \r\n");
        }
        else
            out.write(" \r\n \r\n \r\n");

        out.flush();
    }

    /**
     * notifyIfDelete is a method to send a notification to a server about a server-to-remove to reallocate data. Invoked from ECS
     */
    public void notifyIfDelete(String current, String next, String nextNext){
        out.write("DeletingAServer\r\n" + current + "\r\n" + next + "\r\n" + nextNext + "\r\n");
        out.flush();
    }

    /**
     * updateReps is a method to send a notification to a server, that has replicas to update. Invoked from ECS
     */
    public void updateReps(String command, String rep1, String rep2){
        out.write(command + "\r\n" + rep1 + "\r\n" + rep2 + "\r\n");
        out.flush();
    }

    public String hashMD5(String key) throws NoSuchAlgorithmException {
        MessageDigest msg = MessageDigest.getInstance("MD5");
        byte[] digested = msg.digest(key.getBytes(StandardCharsets.ISO_8859_1));
        String myHash = new BigInteger(1, digested).toString(16);

        return myHash;
    }

    public String getIP(){return this.ip;}

    public int getPort(){return port;}

    public String getHash(){return hash;}

}