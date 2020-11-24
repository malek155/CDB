package de.tum.i13.server.kv;

import java.util.Map;

public interface Cache {

	public void put(String key, String value);

	public String get(String key);

	public void removeKey(String key);

	public boolean containsKey(String key);

}