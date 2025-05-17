package database;

import query.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    // TODO: example to show rollback is working

    public static void main(String[] args) {
        ExecutionEngine engine = new ExecutionEngine(new DataManager());
        TransactionManager tm = new TransactionManager(engine);

        Parser parser = new Parser();
        Instruction instruction = parser.parse("insert K1 Number 13");
        Instruction instruction3 = parser.parse("insert K1 Number2 212");
        Instruction instruction2 = parser.parse("insert K2 N1 1");
        Instruction instruction4 = parser.parse("VISUALISE K1");

        tm.runTransaction(instruction, instruction3, instruction2, instruction4);
    }
}