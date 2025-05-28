package query;

import java.util.*;

public class DataManagerMVCC implements IDataManager {
    private final Map<String, Map<String, Integer>> dataStore = new HashMap<>();

    private Map<String, Map<String, Integer>> deepCopy(Map<String, Map<String, Integer>> original) {
        Map<String, Map<String, Integer>> copy = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> entry : original.entrySet()) {
            copy.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
        return copy;
    }

    public Map<String, Map<String, Integer>> getSnapshot() {
        return deepCopy(dataStore);
    }

    public Map<String, Map<String, Integer>> create(String key) {
        return create(key, dataStore);
    }

    public Map<String, Map<String, Integer>> create(String key, Map<String, Map<String, Integer>> store) {
        Map<String, Map<String, Integer>> version = deepCopy(store);
        version.putIfAbsent(key, new HashMap<>());
        return version;
    }

    // renamed from insertDelta for Interfacing
    public Map<String, Map<String, Integer>> insert(String key,
                                                    Map<String, Integer> value,
                                                    Map<String, Map<String, Integer>> snapshot) {
        Map<String, Map<String, Integer>> delta = new HashMap<>();
        Map<String, Integer> rowDelta = new HashMap<>();

        if (!snapshot.containsKey(key)) {
            delta.put(key, new HashMap<>(value));
        } else {
            Map<String, Integer> row = snapshot.get(key);
            for (Map.Entry<String, Integer> entry : value.entrySet()) {
                String col = entry.getKey();
                Integer newVal = entry.getValue();
                if (!Objects.equals(row.get(col), newVal)) {
                    rowDelta.put(col, newVal);
                }
            }
            if (!rowDelta.isEmpty()) {
                delta.put(key, rowDelta);
            }
        }

        return delta;
    }

    // rename from updateDelta for interfacing
    public Map<String, Map<String, Integer>> update(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> snapshot) {
        return insert(key, value, snapshot);
    }

    // rename from deleteDelta for interfacing
    public Map<String, Map<String, Integer>> delete(String key, Map<String, Integer> value, Map<String, Map<String, Integer>> snapshot) {
        Map<String, Map<String, Integer>> delta = new HashMap<>();

        if (value == null) {
            if (snapshot.containsKey(key)) {
                delta.put(key, null);
            }
        } else if (snapshot.containsKey(key)) {
            Map<String, Integer> row = snapshot.get(key);
            Map<String, Integer> rowDelta = new HashMap<>();
            for (String col : value.keySet()) {
                if (row.containsKey(col)) {
                    rowDelta.put(col, null);
                }
            }
            if (!rowDelta.isEmpty()) {
                delta.put(key, rowDelta);
            }
        }

        return delta;
    }

    public void applySnapshot(Instruction instruction) {
        Map<String, Map<String, Integer>> expected = instruction.getPreOperation();
        Map<String, Map<String, Integer>> delta = instruction.getDelta();

        for (String key : expected.keySet()) {
            if (!Objects.equals(expected.get(key), dataStore.get(key))) {
                throw new IllegalStateException("Conflict on key: " + key);
            }
        }

        for (Map.Entry<String, Map<String, Integer>> entry : delta.entrySet()) {
            String key = entry.getKey();
            Map<String, Integer> changes = entry.getValue();

            if (changes == null) {
                dataStore.remove(key);
                continue;
            }

            dataStore.putIfAbsent(key, new HashMap<>());
            Map<String, Integer> targetRow = dataStore.get(key);

            for (Map.Entry<String, Integer> change : changes.entrySet()) {
                if (change.getValue() == null) {
                    targetRow.remove(change.getKey());
                } else {
                    targetRow.put(change.getKey(), change.getValue());
                }
            }
        }
    }

    public void restoreSnapshot(Instruction instruction) {
        Map<String, Map<String, Integer>> snapshot = instruction.getPreOperation();
        dataStore.clear();
        for (Map.Entry<String, Map<String, Integer>> entry : snapshot.entrySet()) {
            dataStore.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
    }

    public Map<String, Integer> select(String key) {
        return dataStore.getOrDefault(key, new HashMap<>());
    }

    public Map<String, Integer> getRowData(String key) {
        return dataStore.get(key);
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
