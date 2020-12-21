package de.tum.i13;

import de.tum.i13.server.kv.Cache;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVStoreProcessor;
import de.tum.i13.shared.Metadata;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * TestKVCommandProcessor class to handle the JUnit tests
 * 
 * @author gr9
 *
 */
public class TestKVCommandProcessor {
	@Test
	public void correctParsingOfPut() throws Exception {

		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		Cache cache = mock(Cache.class);

		// casting must be added
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, null, "", 0);
		kvcp.process("put key0 value0");

		verify(kv).put("key0", "value0", "");
	}

	@Test
	public void correctParsingOfGet() throws Exception {
		Cache cache = mock(Cache.class);
		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, null, "", 0);
		kvcp.process("get key0");

		verify(kv).get("key0");
	}

	@Test
	public void correctUpdateValue() throws Exception {
		Cache cache = mock(Cache.class);
		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, null, "", 0);
		kvcp.process("put key0 value1");

		verify(kv).put("key0", "value1", "");
	}

	@Test
	public void correctDelete() throws Exception {

		Cache cache = mock(Cache.class);
		KVStoreProcessor kv = mock(KVStoreProcessor.class);
		KVCommandProcessor kvcp = new KVCommandProcessor(kv, cache, null, "", 0);
		kvcp.process("put key0, null");

		verify(kv).put("key0", null, "");
	}

}
