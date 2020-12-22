package de.tum.i13;

import de.tum.i13.server.ecs.ECS;
import de.tum.i13.server.kv.Cache;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVStoreProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * TestKVCommandProcessor class to handle the JUnit tests
 *
 * @author gr9
 */
public class KVCommandProcessorTest {
	//mock objects
	KVStoreProcessor kv = mock(KVStoreProcessor.class);
	ECS ecs = mock(ECS.class);
	Cache cache = mock(Cache.class);
	String testIP = "192.168.1.1";
	int testPort = 5152;

	@Test
	public void correctParsingOfPut() throws Exception {
		// metadata?
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, testIP, testPort);
		kvcp.process("put key0 value0");

		verify(kv).put("key0", "value0", ecs.hashMD5("key0"));
	}

	@Test
	public void correctParsingOfGet() throws Exception {
		Cache cache = mock(Cache.class);
		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, testIP, testPort);
		kvcp.process("get key0");

		verify(kv).get("key0");
	}

	@Test
	public void correctUpdateValue() throws Exception {
		Cache cache = mock(Cache.class);
		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, testIP, testPort);
		kvcp.process("put key0 value1");
		verify(kv).put("key0", "value1", ecs.hashMD5("key0"));
	}

	@Test
	public void correctDelete() throws Exception {

		Cache cache = mock(Cache.class);
		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, testIP, testPort);
		kvcp.process("put key0 null");
		verify(kv).put("key0", null, ecs.hashMD5("key0"));
	}

	@Test
	public void correctHash() throws NoSuchAlgorithmException {
		String testStr = "";
		//this is the hash value of an empty string with MD5
		String hashVal = "d41d8cd98f00b204e9800998ecf8427e";

		ECS ecs = mock(ECS.class);
		Assertions.assertTrue(ecs.hashMD5(testStr).equals(hashVal));
	}


}
