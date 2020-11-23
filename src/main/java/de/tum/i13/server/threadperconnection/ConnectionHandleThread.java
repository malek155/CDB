package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ConnectionHandleThread extends Thread {

    private CommandProcessor cp;
    private Socket clientSocket;
    // private boolean isActive = false;
    BufferedReader in = null;
    PrintWriter out = null;
    InetSocketAddress remote = null;

    public ConnectionHandleThread(CommandProcessor commandProcessor, Socket clientSocket) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        boolean done = true;
        while (!clientSocket.isClosed()) {
            try {
                in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
                out = new PrintWriter(
                        new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));
                // first we call the connection accepted method of the commandprocessor
                remote = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
                // So that we are sending the connectionaccepted msg only once in the beginning
                if (done) {
                    String firstMsg = cp.connectionAccepted(new InetSocketAddress(clientSocket.getLocalPort()), remote);
                    out.write(firstMsg);
                    out.flush();
                    done = false;
                }
                String firstLine;
                while ((firstLine = in.readLine()) != null) {
                    // lehne bech takra el message eli jey mel client ou ta3malou l process
                    String res = cp.process(firstLine);
                    // tab3eth el resultat mte3 el process lil serveur
                    out.write(res);
                    out.flush();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                // handle the exception and add finally block to close everything
            }
        }
        // We display the disconnection notification
        // we maybe have to add sysout in te connectionClosed method in echoLogic
        cp.connectionClosed(remote.getAddress());

    }
}