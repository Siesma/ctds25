package query;

public class Timestamp {

    private final long date;

    public Timestamp () {
        this.date = System.currentTimeMillis();
    }

    public static Timestamp get () {
        return new Timestamp();
    }

}
