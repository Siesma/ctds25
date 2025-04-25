package database;

public class IntegerOperation implements Operation<Integer> {

    @Override
    public void apply(Object original, Object newObject, Constraint<Integer>... constraints) {
        assert original instanceof Integer;
        assert newObject instanceof Integer;
        Integer originalInteger = (Integer) original;
        Integer newObjectInteger = (Integer) newObject;



    }
}
