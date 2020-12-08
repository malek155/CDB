package de.tum.i13.server.circle;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * ConnectionHandleThread class that will handle the thread of each client
 *
 * @author gr9
 *
 */
public class ECSConnection implements Runnable {
    private Socket clientSocket;
    private ECS bigECS; // is watching you

    public ECSConnection(Socket clientSocket, ECS bigECS){
        this.clientSocket = clientSocket;
        this.bigECS = bigECS;
    }

    @Override
    public void run() {
        BufferedReader in = null;
        PrintWriter out = null;

        try{
            in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            out = new PrintWriter(
                    new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

            String line;
            while ((line = in.readLine()) != null) {
                String message = this.process(line);
                if(!message.equals("")){
                    out.write(message);
                    out.flush();
                }
            }
        } catch (IOException ie) {
            ie.printStackTrace();
        }
        finally{
            try {
                if(out!=null)
                    out.close();
                if(in!=null) {
                    out.close();
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String process(String line) {
        String reply = "";

        if (line.equals("mayishutdownplz")) {
            boolean may = this.bigECS.shuttingDown();
            reply = (may)?"yesyoumay":"";
        } else if (line.equals("transferred")) {
            this.bigECS.transferred(true);
        }
        return reply;
    }

}
