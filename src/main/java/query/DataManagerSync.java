package query;

import java.util.*;

public class DataManagerSync implements IDataManager {
    private final Map<String, Map<String, Integer>> dataStore = new HashMap<>();

    public Map<String, Map<String, Integer>> getDataStore() {
        return dataStore;
    }

    @Override
    public synchronized Map<String, Map<String, Integer>> insert(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        dataStore.putIfAbsent(key, new HashMap<>());
        dataStore.get(key).putAll(value);
        return Map.of(key, new HashMap<>(value));
    }

    @Override
    public synchronized Map<String, Map<String, Integer>> update(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        return insert(key, value, unused);
    }

    @Override
    public synchronized Map<String, Map<String, Integer>> delete(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
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
    }

    @Override
    public synchronized Map<String, Integer> select(String key) {
        return new HashMap<>(dataStore.getOrDefault(key, new HashMap<>()));
    }

}
