package query;

import java.util.*;

/**
 * Data Manager manages the actual update, insert, delete and retrieval of data. It provides
 * APIs for these CRUD operations
 */

public class DataManager {
    private final Map<String, Map<String, Integer>> dataStore = new HashMap<>(); //table, values can only be integers

    /**
     * insert a new key-value pair into the table
     */
    public void insert(String key, Map<String, Integer> value) {
        dataStore.putIfAbsent(key, new HashMap<>(value));
    }

    public void delete(String key) {
        dataStore.remove(key);
    }

    /**
     *  select
     * @param key
     * @return column with the given key
     */
    public Map<String, Integer> select(String key) {
        return dataStore.getOrDefault(key, new HashMap<>());
    }

    public Map<String, Integer> getRowData(String key) {
        return dataStore.get(key);
    }

    public Map<String, Map<String, Integer>> getSnapshot() {
        Map<String, Map<String, Integer>> snapshot = new HashMap<>();
        for (String key : dataStore.keySet()){
            snapshot.put(key, new HashMap<>(dataStore.get(key)));
        }
        return snapshot;
    }
}
