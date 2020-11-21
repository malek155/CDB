package de.tum.i13.server.echo;

import java.io.*;
import java.net.Socket;
import java.rmi.server.RemoteCall;

public class ClientConnection implements Runnable{
private Socket clientSocket;
private boolean isOpen;
    public ClientConnection(Socket clientSocket){
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    @Override
    public void run() {
        try{
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(this.clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(this.clientSocket.getOutputStream()));
            while(isOpen){
                try{
                    String clientCommand = in.readLine();
                    System.out.println("Client said: " + clientCommand);
                    out.println("Server: " + clientCommand);
                    out.flush();
                }catch (IOException ioe){
                    System.out.println(ioe);
                }
                finally {
                    try{
                        in.close();
                        out.close();
                        this.clientSocket.close();
                        System.out.println("Stopped");
                    }
                    catch(IOException ioe){
                        System.out.println(ioe);
                    }
                }
            }
        } catch (IOException ioe) {
            isOpen = false;
        }
    }
}
