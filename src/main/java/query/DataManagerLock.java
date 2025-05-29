package query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataManagerLock implements IDataManager {
    private final Map<String, Map<String, Integer>> dataStore = new ConcurrentHashMap<>();
    private final Map<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public Map<String, Map<String, Integer>> getDataStore () {
        return dataStore;
    }

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
            if (!dataStore.containsKey(key)) return new HashMap<>();

            Map<String, Map<String, Integer>> result = new HashMap<>();

            if (value == null) {
                dataStore.remove(key);
                result.put(key, null);
            } else {
                Map<String, Integer> row = dataStore.get(key);
                Map<String, Integer> deletedCols = new HashMap<>();
                for (String col : value.keySet()) {
                    if (row.containsKey(col)) {
                        deletedCols.put(col, null);
                        row.remove(col);
                    }
                }
                result.put(key, deletedCols);
            }

            return result;
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


}
