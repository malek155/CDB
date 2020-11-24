package de.tum.i13;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.KVStoreProcessor;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestKVCommandProcessor {
    @Test
    public void correctParsingOfPut() throws Exception {

        KVStore kv = mock(KVStore.class);

        //casting must be added
        KVCommandProcessor kvcp = new KVCommandProcessor((KVStoreProcessor) kv);
        kvcp.process("put key0 value0");

        verify(kv).put("key0", "value0");
    }

    @Test
    public void correctParsingOfGet() throws Exception {

        KVStore kv = mock(KVStore.class);
        KVCommandProcessor kvcp = new KVCommandProcessor((KVStoreProcessor) kv);
        kvcp.process("get key0");

        verify(kv).get("key0");
    }

    @Test
    public void correctUpdateValue() throws Exception {

        KVStore kv = mock(KVStore.class);
        KVCommandProcessor kvcp = new KVCommandProcessor((KVStoreProcessor) kv);
        kvcp.process("put key0 value1");

        verify(kv).put("key0", "value1");
    }

    @Test
    public void correctDelete() throws Exception {

        KVStore kv = mock(KVStore.class);
        KVCommandProcessor kvcp = new KVCommandProcessor((KVStoreProcessor) kv);
        kvcp.process("delete key0");

        verify(kv).put("key0", null);
    }


}
