package query;

import java.util.*;

public class DataManager {
    private final Map<String, List<Map<String, String>>> dataStore = new HashMap<>();

    public void insert(String table, Map<String, String> row) {
        dataStore.putIfAbsent(table, new ArrayList<>());
        dataStore.get(table).add(new HashMap<>(row));
    }

    public List<Map<String, String>> select(String table) {
        return dataStore.getOrDefault(table, new ArrayList<>());
    }

    public List<Map<String, String>> getTableData(String table) {
        return dataStore.get(table);
    }

    public Map<String, List<Map<String, String>>> getSnapshot() {
        Map<String, List<Map<String, String>>> snapshot = new HashMap<>();
        for (String table : dataStore.keySet()) {
            snapshot.put(table, new ArrayList<>(dataStore.get(table)));
        }
        return snapshot;
    }
}
