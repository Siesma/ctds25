package query;

public class Timestamp {

    private final long date;

    public Timestamp () {
        this.date = System.currentTimeMillis();
    }

    public static Timestamp get () {
        return new Timestamp();
    }

    public long getTime() {
        return date;
    }

    @Override
    public String toString() {
        return String.format("%s", date);
    }
}
