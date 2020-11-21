package de.tum.i13.server.kv;

import java.util.LinkedHashMap;
import java.util.Map;

public class LFUCache implements Cache{
    private class CacheEntry{
        public String value;
        public int frequency;
        public CacheEntry(String value, int frequency){
            this.frequency = frequency;
            this.value = value;
        }
    }

    Map<String, CacheEntry> cache;
    private int size;

    // if accessOrder is true -> lru, after get() visited elements move to the end of the list
    // if accessOrder is false -> fifo principle
    public LFUCache(int size) {
        this.size = size;
        cache = new LinkedHashMap<String, CacheEntry>(size, 1);
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isFull(){
        return this.cache.size() >= this.size;
    }

    public String getToRemove(){
        String key = "";
        int minFreq = Integer.MAX_VALUE;

        for(Map.Entry<String, CacheEntry> entry : cache.entrySet()){
            if(minFreq > entry.getValue().frequency){
                key = entry.getKey();
                minFreq = entry.getValue().frequency;
            }
        }
        return key;
    }

    public synchronized void put(String key, String value){
        if (this.isFull()){
            String keyToRemove = this.getToRemove();
            this.cache.remove(keyToRemove);
        }
        CacheEntry cacheEntry = new CacheEntry(value, 0);
        cache.put(key, cacheEntry);
    }

    public synchronized String get(String key){
        if (this.cache.containsKey(key)) {
            CacheEntry nomiss = cache.get(key);
            nomiss.frequency++;
            return nomiss.value;
        }
        else return null;
    }

    public static void main(String[] args) {
        LFUCache cache = new LFUCache(5);
        System.out.println(cache.cache.size());
        cache.put("0", "0value");
        System.out.println(cache.cache.size());
        cache.put("1", "1value");
        cache.put("2", "2value");
        cache.put("3", "3value");
        cache.put("4", "4value");
        System.out.println(cache.cache.size());
        System.out.println(cache.get("0") + cache.get("1") + cache.get("2") + cache.get("4"));
        cache.put("5", "5value");
        System.out.println(cache.get("3") + cache.get("5"));
        System.out.println(cache.cache.size());

    }
}
