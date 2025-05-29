package query;

import java.util.Map;

public interface IDataManager {

    /*
    ####################
    # General function #
    ####################
     */

    Map<String, Map<String, Integer>> getDataStore();

    default void visualiseDataStore(String key) {
        synchronized (getDataStore()) {
            System.out.printf("%-10s | %-60s%n", "Key", "Value");
            System.out.println("=".repeat(60));

            for (Map.Entry<String, Map<String, Integer>> entry : getDataStore().entrySet()) {
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
