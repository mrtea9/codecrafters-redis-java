

public class KeyValue {
    public String value;
    public long expiryTimestamp;
    public ValueType type;

    public KeyValue(String value, long expiryTimestamp, ValueType type) {
        this.value = value;
        this.expiryTimestamp = expiryTimestamp;
        this.type = type;
    }
}
