package de.tum.i13;

import de.tum.i13.server.kv.KVStoreProcessor;
import de.tum.i13.server.kv.KVCommandProcessor;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestKVCommandProcessor {

    @Test
    public void correctParsingOfPut() throws Exception {

        KVStoreProcessor kv = mock(KVStoreProcessor.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kv);
        kvcp.process("put key hello");

        verify(kv).put("key", "hello");
    }
}
