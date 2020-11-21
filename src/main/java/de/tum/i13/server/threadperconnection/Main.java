package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {
	// used to shut down the server , maybe we need it
	private static boolean isRunning = true;

	// method to close the server
	public void close() {
		isRunning = false;
	}

	public static void main(String[] args) throws IOException {
		Config cfg = parseCommandlineArgs(args); // Do not change this
		setupLogging(cfg.logfile);

		final ServerSocket serverSocket = new ServerSocket();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Closing thread per connection kv server");
				try {
					serverSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		// bind to localhost only
		serverSocket.bind(new InetSocketAddress(cfg.listenaddr, cfg.port));

		// Replace with your Key value server logic.
		// If you use multithreading you need locking
		// we can have
		CommandProcessor logic = new EchoLogic();
		// as we are using the same instance of logic for all the threads then we need
		// only to synchronize the accessed methods , and if we are about to lock an
		// object we have to lock the KVStore object which is only accessed through the
		// KVCommandProcessor the we want have an access to it so I think we can only
		// use the synchronized methods , otherwise we can change it and that will
		// affect the structure that I am working with

		// while (true) {
		while (isRunning) {
			// Waiting for client to connect
			Socket clientSocket = serverSocket.accept();

			// When we accept a connection, we start a new Thread for this connection
			Thread th = new ConnectionHandleThread(logic, clientSocket);
			th.start();
		}

	}
}
