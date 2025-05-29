package query;

import java.util.Map;

public interface IDataManager {

    /*
    ####################
    # General function #
    ####################
     */
    void visualiseDataStore(String table);
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
