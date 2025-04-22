package database;

import query.Instruction;
import query.OperationType;

public class Parser {

    /*
    update myvalue 23467
     */

    public Parser() {

    }

    public Instruction parse(String input) {
        String[] content = input.split(" ");
        OperationType type;
        try {
            type = OperationType.valueOf(content[0]);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        switch (type) {
            case INSERT -> {
                return new Instruction(type, null, null);
            }
            case UPDATE -> {
                return new Instruction(type, null, null);
            }
            case DELETE -> {
                return new Instruction(type, null, null);
            }
            case GET -> {
                return new Instruction(type, null, null);
            }
            case COMMIT -> {
                return new Instruction(type, null, null);
            }
            case ROLLBACK -> {
                return new Instruction(type, null, null);
            }
            default -> {
                return null;
            }
        }
    }

}
