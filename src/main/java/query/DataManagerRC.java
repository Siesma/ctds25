package query;

import java.util.*;

// RC stands for Race Condition, i.e. no concurrency control mechanism implemented
public class DataManagerRC implements IDataManager {
    private final Map<String, Map<String, Integer>> dataStore = new HashMap<>();

    @Override
    public Map<String, Map<String, Integer>> insert(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> unused) {
        dataStore.putIfAbsent(key, new HashMap<>());
        dataStore.get(key).putAll(value);
        return Map.of(key, new HashMap<>(value));
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

    @Override
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
