package query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataManagerLock implements IDataManager {
    private final Map<String, Map<String, Integer>> dataStore = new ConcurrentHashMap<>();
    private final Map<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    private ReentrantReadWriteLock getLock(String key) {
        return locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    public Map<String, Map<String, Integer>> insert(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        ReentrantReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            dataStore.putIfAbsent(key, new HashMap<>());
            Map<String, Integer> row = dataStore.get(key);
            row.putAll(value);
        } finally {
            lock.writeLock().unlock();
        }
        return Map.of(key, new HashMap<>(value));
    }

    public Map<String, Map<String, Integer>> update(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        return insert(key, value, unused);
    }

    public Map<String, Map<String, Integer>> delete(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        ReentrantReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            if (!dataStore.containsKey(key)) return Map.of();

            if (value == null) {
                dataStore.remove(key);
                return Map.of(key, null);
            } else {
                Map<String, Integer> row = dataStore.get(key);
                for (String col : value.keySet()) {
                    row.remove(col);
                }
                return Map.of(key, new HashMap<>(row));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<String, Integer> select(String key) {
        ReentrantReadWriteLock lock = getLock(key);
        lock.readLock().lock();
        try {
            return new HashMap<>(dataStore.getOrDefault(key, new HashMap<>()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void visualiseDataStore(String key) {
        System.out.printf("%-10s | %-60s%n", "Key", "Value");
        System.out.println("=".repeat(60));
        for (Map.Entry<String, Map<String, Integer>> entry : dataStore.entrySet()) {
            String outerKey = entry.getKey();
            if (!(outerKey.equalsIgnoreCase(key) || key.equals("*"))) {
                continue;
            }
            Map<String, Integer> innerMap = entry.getValue();
            System.out.printf("%-10s | ", outerKey);
            StringBuilder innerBuilder = new StringBuilder();
            for (Map.Entry<String, Integer> innerEntry : innerMap.entrySet()) {
                innerBuilder.append(String.format("[%-8s: %-5d] ", innerEntry.getKey(), innerEntry.getValue()));
            }
            System.out.println(innerBuilder.toString().trim());
        }
    }

}
