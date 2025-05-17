package database;

public enum Operator {
    ADD("+"),
    SUBTRACT("-"),
    MULTIPLY("*"),
    DIVIDE("/");

    private final String symbol;

    Operator(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }
}