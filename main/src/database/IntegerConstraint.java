package database;

public class IntegerConstraint implements Constraint<Integer> {

    public Integer apply (Integer before, Integer after, String type) {
        return 0;
    }


    private Integer applyMin (Integer before, Integer after) {
        return Math.min(before, after);
    }

}
