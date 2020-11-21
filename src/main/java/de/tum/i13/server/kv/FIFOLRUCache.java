package de.tum.i13.server.kv;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOLRUCache implements  Cache{
    Map<String, String> cache;
    private static int size;

    // if accessOrder is true -> lru, after get() visited elements move to the end of the list
    // if accessOrder is false -> fifo principle
    public FIFOLRUCache(int size, boolean accessOrder) {
        this.size = size;
        cache = new LinkedHashMap<String, String>(size, 1, accessOrder) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return this.size() == size;
            }
        };
    }

    public synchronized void put(String key, String value) {
        cache.put(key, value);
    }

    public synchronized String get(String key) {
        return cache.get(key);
    }

    public synchronized void remove(String key) {
        cache.remove(key);
    }
public synchronized void setSize(int size){
        this.size=size;
}

    public synchronized int getSize() {
        return this.size;
    }



    public static void main(String[] args) {
        FIFOLRUCache cache = new FIFOLRUCache(5, true);
        System.out.println(cache.cache.size());
        cache.put("0", "0value");
        System.out.println(cache.cache.size());
        cache.put("1", "1value");
        cache.put("2", "2value");
        cache.put("3", "3value");
        System.out.println(cache.cache.size());
        System.out.println(cache.get("0") + cache.get("1") + cache.get("2"));
        cache.put("5", "5value");
        System.out.println(cache.get("3") + cache.get("5"));
        System.out.println(cache.cache.size());

    }
}
