package query;

import javax.xml.crypto.Data;
import java.util.HashMap;

public class DataManager {

    private HashMap<String, Object> entries;

    public DataManager() {
        init();
    }

    private void init() {
        this.entries = new HashMap<>();

    }

    public Object get(String lookup) {
        return this.entries.get(lookup);
    }

    public void update (String lookup, Object newO) {

    }


}
