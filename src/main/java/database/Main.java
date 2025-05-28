package database;

import query.*;

public class Main {

    public static void main(String[] args) {
        IDataManager dataManager = new DataManagerLock();
        ExecutionEngine engine = new ExecutionEngine(dataManager);
        TransactionManager tm = new TransactionManager(engine);
        Parser parser = new Parser();

        Instruction insert1 = parser.parse("INSERT K1 Number 13");
        Instruction insert2 = parser.parse("INSERT K1 Number2 212");
        Instruction insert3 = parser.parse("INSERT K2 N1 1");
        Instruction visualiseBeforeCommit = parser.parse("VISUALISE *");

        System.out.println("Running initial transaction (pending commit):");
        tm.runTransaction(insert1, insert2, insert3, visualiseBeforeCommit);

        System.out.println("\nCommitting transaction...");
        Instruction commit = new Instruction(QueryType.COMMIT, null, null);
        engine.execute(commit);

        System.out.println("\nState after commit:");
        engine.execute(parser.parse("VISUALISE *"));
    }
}
