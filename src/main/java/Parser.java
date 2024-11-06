import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {

    private final List<String> decodedResponse;
    private final String source;

    public Parser(String source) {
        this.source = source;
        this.decodedResponse = new ArrayList<>();
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
        String result = "*2\r\n$3\r\ndir\r\n$4\r\ntest\r\n";

        return result;
    }
}
