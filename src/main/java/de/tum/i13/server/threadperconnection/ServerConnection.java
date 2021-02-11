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

    /**
     * run method to invoke another connection thread to another server indicating an inner connection
     * for transferring storages and replicas
     */
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

    /**
     * transfer2 method to send 2 files to a corresponding server
     * @param storage file that should be transferred, normally after adding or deleting of a server
     */
    public void transfer(File storage) throws FileNotFoundException {
        if(storage.length() != 0){
            Scanner scanner = new Scanner(new FileInputStream(storage));
            while (scanner.hasNextLine()){
                String line = scanner.nextLine();
                if(!line.trim().equals(""))
                    out.write("transferring " + line +"\r\n");
            }
            out.write("You'reGoodToGo" + "\r\n");
            out.flush();
            scanner.close();
        }
    }

    /**
     * transfer2 method to send 2 files to a corresponding server
     * @param file1,file2 files that should be transferred
     * @param ms how much a thread should wait
     */
    public void transfer2(File file1, File file2, int ms) throws FileNotFoundException, InterruptedException {

        Thread.sleep(ms);
        if(file1.length() != 0){
            Scanner scanner1 = new Scanner(new FileInputStream(file1));

            while (scanner1.hasNextLine()){
                String line = scanner1.nextLine();
                if(!line.trim().equals(""))
                    out.write("replica1 " + line + "\r\n");
            }
            out.flush();
            scanner1.close();
        }

        if(file2.length() != 0) {
            Scanner scanner2 = new Scanner(new FileInputStream(file2));
            while (scanner2.hasNextLine()) {
                String line = scanner2.nextLine();
                if(!line.trim().equals(""))
                    out.write("replica2 " + line + "\r\n");
            }
            out.flush();
            scanner2.close();
        }
    }

    /**
     * transferOne method to send one file to a corresponding server
     * @param file that should be transferred
     * @param fileName can be sent as replica1/replica2/storage
     * @param ms how much a thread should wait
     */
    public void transferOne(File file, String fileName, int ms) throws FileNotFoundException, InterruptedException {

        Thread.sleep(ms);
        if(file.length() != 0) {
            Scanner scanner = new Scanner(new FileInputStream(file));

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if(!line.trim().equals(""))
                    out.write(fileName + " " +line + "\r\n");
            }
            out.flush();
            scanner.close();
        }
    }

}

