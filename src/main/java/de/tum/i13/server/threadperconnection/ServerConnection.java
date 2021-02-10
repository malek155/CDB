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

    public void transfer2(File file1, File file2, boolean wait) throws FileNotFoundException, InterruptedException {
        if(wait)
            Thread.sleep(4000);
        if(file1.length() != 0){
            Scanner scanner1 = new Scanner(new FileInputStream(file1));

//            if(scanner1.hasNextLine()){
//                out.write("replica1 first " + scanner1.nextLine() + "\r\n");
//            }
            while (scanner1.hasNextLine()){
                out.write("replica1 " + scanner1.nextLine() + "\r\n");
            }
            out.flush();
            scanner1.close();
        }

        if(file2.length() != 0) {
            Scanner scanner2 = new Scanner(new FileInputStream(file2));
//            if(scanner2.hasNextLine()){
//                out.write("replica2 first " + scanner2.nextLine() + "\r\n");
//            }
            while (scanner2.hasNextLine()) {
                out.write("replica2 " + scanner2.nextLine() + "\r\n");
            }
            out.flush();
            scanner2.close();
        }
    }

    public void transferOne(File file, String fileName, boolean wait) throws FileNotFoundException, InterruptedException {
        if(wait)
            Thread.sleep(3000);
        if(file.length() != 0) {
            Scanner scanner = new Scanner(new FileInputStream(file));

//            if(scanner.hasNextLine()){
//                out.write("fileName first " + scanner.nextLine() + "\r\n");
//            }

            while (scanner.hasNextLine()) {
                out.write(fileName + " " + scanner.nextLine() + "\r\n");
            }
            out.flush();
            scanner.close();
        }
    }

}

