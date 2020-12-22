package de.tum.i13;

import de.tum.i13.client.Milestone1Main;
import de.tum.i13.server.ecs.ECS;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import static org.mockito.Mockito.mock;

public class ConnectionTest {

	private int port = 5152;
	private String host = "127.0.0.1";
	// servers
	private OutputStream output = null;
	private InputStream input = null;

	@Test
	public void clientServerTest() throws IOException {
		ServerSocket s = new ServerSocket(port);
//listening
		Milestone1Main ms1 = mock(Milestone1Main.class);
//ms1.sendMessage?
		Socket client = new Socket(host, port);
		// clients
		OutputStream out = client.getOutputStream();
		InputStream in = client.getInputStream();

		// client -> server
		out.write(Integer.parseInt("Test1"));
		out.flush();
		assertRight(input, "Test1");

		// server -> client
		output.write(Integer.parseInt("Test2"));
		output.flush();
		assertRight(in, "Test2");

		client.close();
		s.close();
	}

	/**
	 * assertRight helping method reads from an inputStream and checks if it's equal
	 * to the expected value
	 */
	void assertRight(InputStream input, String expectedVal) throws IOException {
		byte[] buffer = new byte[expectedVal.length()];
		input.read(buffer);
		String bufStr = new String(buffer);
		Assertions.assertEquals(expectedVal, bufStr);
	}

}