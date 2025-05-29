package database;

import query.Instruction;
import query.QueryType;

import java.util.HashMap;
import java.util.Map;

public class Parser {

    public Instruction parse(String input) {
        String[] tokens = input.trim().split("\\s+");

        if (tokens.length < 2) {
            System.err.println("Invalid command.");
            return null;
        }

        String command = tokens[0].toUpperCase();

        try {
            QueryType type = QueryType.valueOf(command);

            switch (type) {
                case VISUALISE -> {
                    return new Instruction(QueryType.VISUALISE, tokens[1], new HashMap<>());
                }
                case INSERT, UPDATE -> {
                    String rowKey = tokens[1];
                    String colKey = tokens[2];
                    int value = Integer.parseInt(tokens[3]);

                    Map<String, Integer> rowData = new HashMap<>();
                    rowData.put(colKey, value);
                    return new Instruction(type, rowKey, rowData);
                }

                case DELETE, GET -> {
                    String target = tokens[1];
                    /*
                    Might work like this, maybe not depends on future iteration of Instruction handling
                    if (type == OperationType.GET && "*".equals(target) && tokens.length == 3) {
                        String colKey = tokens[2];
                        Map<String, Integer> wildcardData = new HashMap<>();
                        wildcardData.put("*", 0);
                        wildcardData.put(colKey, 0);
                        return new Instruction(type, "*", wildcardData);
                    }
                    */

                    return new Instruction(type, target, null);
                }
                case COMMIT -> {
                    return new Instruction(QueryType.COMMIT, tokens[1], null);
                }
                case ROLLBACK -> {
                    // TODO: Fix parameter
                    return new Instruction(QueryType.ROLLBACK, tokens[1], null);
                }
                default -> {
                    System.err.println("Unsupported operation: " + command);
                    return null;
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Unknown operation type: " + command);
            return null;
        }
    }
}
