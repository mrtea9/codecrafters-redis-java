import java.util.HashMap;
import java.util.Map;

public class KeyValue {
    public String key;
    public String value;
    public long expiryTimestamp;
    public ValueType type;
    public String entryId;
    public Map<String, KeyValue> entries = new HashMap<>();

    public KeyValue(String value, long expiryTimestamp, ValueType type) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
        this.type = type;
    }

    public KeyValue(String entryId, String key, String value, ValueType type) {
        this.entryId = entryId;
        this.type = type;
        KeyValue keyValue = new KeyValue(key, value);
        addEntry(keyValue);
    }

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
        this.type = ValueType.KEYVALUE;
    }

    public void addEntry(KeyValue keyValue) {
        this.entries.put(this.entryId, keyValue);
    }
}
