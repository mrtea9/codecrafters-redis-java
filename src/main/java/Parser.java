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
        HashMap<String, String> database = extractDatabase(hexFile);

        System.out.println(header);
        System.out.println(metadata);
        System.out.println(database);
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

    private static HashMap<String, String> extractDatabase(ArrayList<String> hexFile) {
        // need to implement errors
        HashMap<String, String> result = new HashMap<>();

        int index = 0;

        index++; // remove initial "FE"
        int databaseIndex = Integer.parseInt(hexFile.get(index++), 16); // getting database index and after that deleting it

        index++; // delete hex "FB"
        int keysTableSize = Integer.parseInt(hexFile.get(index++), 16); // getting keysTableSize and after that deleting it
        int expiresTable = Integer.parseInt(hexFile.get(index++), 16); // getting expiresTable and after that deleting it

        String flag = hexFile.get(index++); // getting flag and after that deleting it

        System.out.println("keys table size = " + keysTableSize);
        System.out.println("flag = " + flag);

        while (!hexFile.get(index).equals("FF")) {

            int keySize = Integer.parseInt(hexFile.get(index++), 16);
            String key = parseHexString(hexFile, index, keySize);
            //System.out.println(keyHex);
            index += keySize;

            int valueSize = Integer.parseInt(hexFile.get(index++), 16);
            String value = parseHexString(hexFile, index, valueSize);
            //System.out.println(valueHex);
            index += valueSize;

            result.put(key, value);
        }

        //System.out.println(result);
        return result;
    }

    private static String parseHexString(ArrayList<String> hexFile, int startIndex, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int decimalValue = Integer.parseInt(hexFile.get(startIndex + i), 16);
            sb.append(Character.toChars(decimalValue));
        }
        return sb.toString();
    }
}
