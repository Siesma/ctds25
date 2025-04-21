package database;

public interface Operation {

    void apply (Object original, Object newObject, Constraint... constraints);

}
