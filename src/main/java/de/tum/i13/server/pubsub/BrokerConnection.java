package de.tum.i13.server.pubsub;

import de.tum.i13.shared.Constants;

import java.io.*;
import java.net.Socket;
import java.util.logging.Logger;

public class BrokerConnection implements Runnable {
    private PrintWriter out;
    private String ip;
    private int port;

    public BrokerConnection(String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        Socket socket = new Socket(this.ip, this.port);
        out = new PrintWriter(
                new OutputStreamWriter(socket.getOutputStream(), Constants.TELNET_ENCODING));
    }

    @Override
    public void run(){
        try{

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }

    /**
     * method notifyOne is invoked from Broker and is used for notifying one single client
     * @param notification a publication notification
     */
    public void notifyOne(String notification){
        if(out != null){
            out.write(notification + "\r\n");
            out.flush();
        }
    }

    public String getIp(){
        return this.ip;
    }

    public int getPort(){
        return this.port;
    }


    }
