import java.nio.charset.StandardCharsets;
import java.util.*;

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
        String metadata = extractMetadata(hexFile);
        String database = extractDatabase(hexFile);

        //System.out.println(header);
        //System.out.println(metadata);
        System.out.println(hexFile);
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
            header.append(Character.toChars(decimalValue));
        }

        if (!header.isEmpty()) {
            hexFile.subList(0, header.length()).clear();
        }

        return header.toString();
    }

    private static String extractMetadata(ArrayList<String> hexFile) {
        StringBuilder metadata = new StringBuilder();

        for (int i = 0; i < hexFile.size(); i++) {
            String hex = hexFile.get(i);

            if (hex.equals("FE")) break;
            if (hex.equals("FA")) continue;
            if (hex.equals("05")) {
                metadata.append(" ");
                continue;
            }
            if (hex.equals("C0")) {
                int number = Integer.parseInt(hexFile.get(i + 1), 16);
                metadata.append(" ");
                metadata.append(number);
                hexFile.remove(0);
                continue;
            }

            int decimalValue = Integer.parseInt(hex, 16);
            metadata.append(Character.toChars(decimalValue));

            //System.out.println("hex = " + hex + "; char = " + Arrays.toString(Character.toChars(decimalValue)));
        }

        if (!metadata.isEmpty()) {
            hexFile.subList(0, metadata.length()).clear();
        }

        return metadata.toString();
    }

    private static String extractDatabase(ArrayList<String> hexFile) {
        // need to implement errors
        HashMap<String, String> result = new HashMap<>();

        hexFile.remove(0); // delete hex "FE"
        int databaseIndex = Integer.parseInt(hexFile.get(0), 16);
        hexFile.remove(0); // delete database index

        hexFile.remove(0); // delete hex "FB"
        int keysTableSize = Integer.parseInt(hexFile.get(0), 16);
        hexFile.remove(0); // delete size of the hash table that stores the keys and values (size encoded)

        int expiresTable = Integer.parseInt(hexFile.get(0), 16);
        hexFile.remove(0); // delete size of the hash table that stores the expires of keys (size encoded)

        String flag = hexFile.get(0);
        hexFile.remove(0); // delete flag

        System.out.println("keys table size = " + keysTableSize);
        System.out.println("flag = " + flag);

        int keyLength = Integer.parseInt(hexFile.get(0), 16);
        hexFile.remove(0);

        StringBuilder key = new StringBuilder();
        for (String hex : hexFile) {
            if (key.length() == keyLength) break;

            int decimalValue = Integer.parseInt(hex, 16);
            key.append(Character.toChars(decimalValue));
        }
        System.out.println(key.toString());
        System.out.println(hexFile);
        return "";
    }
}
