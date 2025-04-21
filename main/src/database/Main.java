package database;

import query.DataManager;
import query.ExecutionEngine;
import query.OperationType;
import query.TransactionManager;

import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        ExecutionEngine engine = new ExecutionEngine(new DataManager());
        TransactionManager tm = new TransactionManager(engine);

        Map<String, String> row1 = Map.of("id", tm.obtainNewUUID(), "name", "Alice");
        Map<String, String> row2 = Map.of("id", tm.obtainNewUUID(), "name", "Bob");

        System.out.println(row1);
        System.out.println(row2);

        tm.runTransaction(
            new TransactionManager.Instruction(OperationType.INSERT, "users", row1),
            new TransactionManager.Instruction(OperationType.INSERT, "users", row2),
            new TransactionManager.Instruction(OperationType.SELECT, "users", null)
        );
    }
}