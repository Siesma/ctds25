package query;

public enum QueryType {
    /*
    Insert, Update and Delete are volatile / critical operations that require a commit before they are actively
    appended. They only create, allocate and preempt the necessary regions data structures before they are finally
    attached into the actual database, which happens with a "commit".
     */

    INSERT(String.class, String.class, Integer.class),
    UPDATE(),
    DELETE(),
    GET(),
    INCREMENT(),
    DECREMENT(),
    COMMIT(String.class),
    ROLLBACK(String.class),
    VISUALISE(String.class);
    QueryType(Class<?>... instructionParameters) {
        /*
        not used, the classes can be considered to be a reminder how the operations have to be formed
         */
    }
}
