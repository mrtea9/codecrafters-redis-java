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
        ArrayList<String> hexFile = bytesToHex(bytes);

        System.out.println(hexFile);

        String header = extractHeader(hexFile);
        //String metadata = extractMetadata(hexFile);

        System.out.println(hexFile);
        System.out.println(header);
    }

    private static ArrayList<String> bytesToHex(byte[] bytes) {
        ArrayList<String> hexResult = new ArrayList<>();

        for (byte b : bytes) {
            hexResult.add(String.format("%02X", b));
        }

        return hexResult;
    }

    private static String extractHeader(ArrayList<String> hexFile) {
        StringBuilder header = new StringBuilder();

        for (String hex : hexFile) {
            if (hex.equals("FA")) break;

            int decimalValue = Integer.parseInt(hex, 16);
            System.out.println("hex = " + hex + "; char = " + Arrays.toString(Character.toChars(decimalValue)));
            header.append(Character.toChars(decimalValue));
        }

        if (!header.isEmpty()) {
            hexFile.subList(0, header.length()).clear();
        }

        return header.toString();
    }

    private static String extractMetadata(ArrayList<String> hexFile) {

        return "";
    }
}
