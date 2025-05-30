package query;

import java.util.*;

// RC stands for Race Condition, i.e. no concurrency control mechanism implemented
public class DataManagerRC implements IDataManager {
    private final Map<String, Map<String, Integer>> dataStore = new HashMap<>();
    public Map<String, Map<String, Integer>> getDataStore () {
        return dataStore;
    }

    @Override
    public Map<String, Map<String, Integer>> insert(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        dataStore.putIfAbsent(key, new HashMap<>());
        dataStore.get(key).putAll(value);
        return Map.of(key, new HashMap<>(value));
    }

    @Override
    public Map<String, Map<String, Integer>> decrement(String table, Map<String, Integer> row, Map<String, Map<String, Integer>> snapshot) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Map<String, Integer>> increment(String table, Map<String, Integer> row, Map<String, Map<String, Integer>> snapshot) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Map<String, Integer>> update(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        return insert(key, value, unused);
    }

    @Override
    public Map<String, Map<String, Integer>> delete(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
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
    public Map<String, Integer> select(String key) {
        return new HashMap<>(dataStore.getOrDefault(key, new HashMap<>()));
    }
}
