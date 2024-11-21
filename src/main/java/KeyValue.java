

public class KeyValue {
    public String key;
    public String value;
    public long expiryTimestamp;
    public ValueType type;
    public String entryId;

    public KeyValue(String value, long expiryTimestamp, ValueType type) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
        this.type = type;
    }

    public KeyValue(String entryId, String key, String value, ValueType type) {
        this.entryId = entryId;
        this.key = key;
        this.value = value;
        this.type = type;
    }
}
