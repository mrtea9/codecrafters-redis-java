import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {

    private final List<String> decodedResponse = new ArrayList<>();
    private final String source;

    public Parser() {
        this.source = "";
    }

    public Parser(String source) {
        this.source = source;
    }

    public List<String> getDecodedResponse() {
        return this.decodedResponse;
    }

    public void parse() {
        String[] intermediate = this.source.split("\r\n");
        String[] sourceList = Arrays.copyOf(intermediate, intermediate.length - 1) ;

        parseSourceList(sourceList);
    }

    private void parseSourceList(String[] sourceList) {
        for (String element : sourceList) {
            if (element.charAt(0) == '*') continue;

            if (element.charAt(0) == '$') continue;

            if (element.charAt(0) == ':') continue;

            this.decodedResponse.add(element);
        }
    }

    public static String encodeArray(String key, String value) {
        int keyLength = key.length();
        int valueLength = value.length();
        String result = "*2\r\n$" + keyLength + "\r\n" + key + "\r\n$" + valueLength + "\r\n" + value + "\r\n";

        return result;
    }

    public static void parseRdbFile(byte[] bytes) {

        String[] hexString = bytesToHex(bytes);

        for (String hex : hexString) {
            System.out.println("hex = " + hex);
        }

        System.out.println(Arrays.toString(hexString));
    }

    private static String[] bytesToHex(byte[] bytes) {
        String[] hexResult = new String[bytes.length];

        for (int i = 0; i < bytes.length; i++) {
            hexResult[i] = String.format("%02X", bytes[i]);
        }

        return hexResult;
    }
}
