package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.Constants;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;
import java.util.logging.Logger;

public class ServerConnection implements Runnable {
    private PrintWriter out;
    private String ip;
    private int port;

    public static Logger logger = Logger.getLogger(de.tum.i13.server.ecs.ECSConnection.class.getName());

    public ServerConnection(String ip, int port) throws IOException {
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

    public String getIp(){
        return this.ip;
    }

    public int getPort(){
        return this.port;
    }

    public void transfer(File storage) throws FileNotFoundException {
        if(storage.length() != 0){
            Scanner scanner = new Scanner(new FileInputStream(storage));
            while (scanner.hasNextLine()){
                String transfer = "transferring " + scanner.nextLine();
                out.write(transfer+"\r\n");
            }
            out.write("You'reGoodToGo" + "\r\n");
            out.flush();
            scanner.close();
        }
    }

    public void transfer2(File file1, File file2) throws FileNotFoundException {
        Scanner scanner1 = new Scanner(new FileInputStream(file1));
        Scanner scanner2 = new Scanner(new FileInputStream(file2));

        while (scanner1.hasNextLine()){
            out.write("replica1 " + scanner1.nextLine() + "\r\n");
        }
        while (scanner2.hasNextLine()){
            out.write("replica2 " + scanner1.nextLine() + "\r\n");
        }
        out.flush();
        scanner1.close();
        scanner2.close();
    }

    public void transferOne(File file) throws FileNotFoundException {
        Scanner scanner = new Scanner(new FileInputStream(file));

        while (scanner.hasNextLine()){
            out.write("replica2 " + scanner.nextLine() + "\r\n");
        }
        out.flush();
        scanner.close();
    }


}

