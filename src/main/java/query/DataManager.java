package query;

import java.util.*;

/**
 * Data Manager manages the actual update, insert, delete and retrieval of data. It provides
 * APIs for these CRUD operations
 */

public class DataManager {
    private final Map<String, Map<String, Integer>> dataStore = new HashMap<>(); //table, values can only be integers


    public void create(String key) {
        dataStore.putIfAbsent(key, new HashMap<>());
    }

    /**
     * insert a new key-value pair into the table
     */
    public void insert(String key, Map<String, Integer> value) {
        if (!dataStore.containsKey(key)) {
            create(key);
        }
        value.forEach((k, v) -> {
            dataStore.get(key).put(k, v);
        });
    }

    public void update(String key, Map<String, Integer> value) {
        if (!dataStore.containsKey(key)) {
            create(key);
        }
        insert(key, value);
    }

    public void delete(String key, Map<String, Integer> value) {
        if (value == null) {
            dataStore.remove(key);
            return;
        }
        value.forEach((k, v) -> {
            dataStore.remove(key);
        });
    }

    /**
     * select
     *
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
        for (String key : dataStore.keySet()) {
            snapshot.put(key, new HashMap<>(dataStore.get(key)));
        }
        return snapshot;
    }

    public void visualiseDataStore(String key) {
        System.out.printf("%-10s | %-60s%n", "Key", "Value");
        System.out.println("=".repeat(60));

        for (Map.Entry<String, Map<String, Integer>> entry : dataStore.entrySet()) {
            String outerKey = entry.getKey();
            if(!(outerKey.equalsIgnoreCase(key) || key.equals("*"))) {
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
