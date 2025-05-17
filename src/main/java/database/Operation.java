package database;

public class Operation {

    private Operator operator;
    private Integer value;

    public Operation(Operator operator, Integer value) {
        this.operator = operator;
        this.value = value;
    }

    public Integer apply(Integer originalValue) {
        // TODO: error handling

        switch (operator) {
            case ADD:
                return originalValue + value;
            case SUBTRACT:
                return originalValue - value;
            case MULTIPLY:
                return originalValue * value;
            case DIVIDE:
                return value != 0 ? originalValue / value : originalValue;
            default:
                throw new UnsupportedOperationException("Unknown operator: " + operator);
        }
    }
}
