import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
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
        String[] hexFile = bytesToHex(bytes);

        String header = extractHeader(hexFile);
        String metadata = extractMetadata(hexFile);

        System.out.println(Arrays.toString(hexFile));
        System.out.println(header);
    }

    private static String[] bytesToHex(byte[] bytes) {
        String[] hexResult = new String[bytes.length];

        for (int i = 0; i < bytes.length; i++) {
            hexResult[i] = String.format("%02X", bytes[i]);
        }

        return hexResult;
    }

    private static String extractHeader(String[] hexFile) {
        StringBuilder header = new StringBuilder();

        for (String hex : hexFile) {
            if (hex.equals("FA")) return header.toString();

            int decimalValue = Integer.parseInt(hex, 16);
            System.out.println("hex = " + hex + "; char = " + Arrays.toString(Character.toChars(decimalValue)));
            header.append(Character.toChars(decimalValue));
        }

        return header.toString();
    }

    private static String extractMetadata(String[] hexFile) {

    }
}
