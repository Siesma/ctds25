package query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface IDataManager {

    /*
    ####################
    # General function #
    ####################
     */

    Map<String, Map<String, Integer>> getDataStore();

    default void visualiseDataStore(String key) {
        System.out.printf("%-10s | %-60s%n", "Key", "Value");
        System.out.println("=".repeat(60));

        Map<String, Map<String, Integer>> safeSnapshot = new ConcurrentHashMap<>();

        getDataStore().forEach((outerKey, innerMap) -> {
            Map<String, Integer> innerCopy = new ConcurrentHashMap<>();
            innerMap.forEach((k, v) -> innerCopy.put(k, v));
            safeSnapshot.put(outerKey, innerCopy);
        });

        for (Map.Entry<String, Map<String, Integer>> entry : safeSnapshot.entrySet()) {
            String outerKey = entry.getKey();
            if (!(outerKey.equalsIgnoreCase(key) || key.equals("*"))) {
                continue;
            }

            System.out.printf("%-10s | ", outerKey);
            StringBuilder innerBuilder = new StringBuilder();

            for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
                innerBuilder.append(String.format("[%-8s: %-5d] ", innerEntry.getKey(), innerEntry.getValue()));
            }

            System.out.println(innerBuilder.toString().trim());
        }
    }

    Map<String, Integer> select(String table);


    /*
    ##########################
    # Mostly General function #
    ###########################
     */

    Map<String, Map<String, Integer>> update(
            String table,
            Map<String, Integer> row,
            Map<String, Map<String, Integer>> snapshot /*not necessary for lock based*/
    );

    Map<String, Map<String, Integer>> delete(
            String table,
            Map<String, Integer> row,
            Map<String, Map<String, Integer>> snapshot /*not necessary for lock based*/
    );

    Map<String, Map<String, Integer>> insert(
            String table,
            Map<String, Integer> row,
            Map<String, Map<String, Integer>> snapshot /*not necessary for lock based*/
    );

    Map<String, Map<String, Integer>> decrement(
            String table,
            Map<String, Integer> row,
            Map<String, Map<String, Integer>> snapshot /*not necessary for lock based*/
    );

    Map<String, Map<String, Integer>> increment(
            String table,
            Map<String, Integer> row,
            Map<String, Map<String, Integer>> snapshot /*not necessary for lock based*/
    );

    /*
    ###########################
    # MVCC Exclusive function #
    ###########################
     */
    default Map<String, Map<String, Integer>> getSnapshot() {
        throw new UnsupportedOperationException(getErrorMessage("getSnapshot"));
    }

    default void applySnapshot(Instruction instruction) {
        throw new UnsupportedOperationException(getErrorMessage("applySnapshot"));
    }

    default void restoreSnapshot(Instruction instruction) {
        throw new UnsupportedOperationException(getErrorMessage("restoreSnapshot"));
    }

    private String getErrorMessage(String method) {
        return String.format("Method %s is only supported in MVCC manager.", method);
    }
}
