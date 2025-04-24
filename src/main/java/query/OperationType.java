package query;

public enum OperationType {
    /*
    Insert, Update and Delete are volatile / critical operations that require a commit before they are actively
    appended. They only create, allocate and preempt the necessary regions data structures before they are finally
    attached into the actual database, which happens with a "commit".
     */

    INSERT,
    UPDATE,
    DELETE,
    GET,
    COMMIT,
    ROLLBACK,
    VISUALISE;
}
