import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {

    private List<String> decodedResponse;
    private final String source;

    public Parser(String source) {
        this.source = source;
        this.decodedResponse = new ArrayList<>();
    }

    public void parse() {
        String[] intermediate = this.source.split("\r\n");
        String[] sourceList = Arrays.copyOf(intermediate, intermediate.length - 1) ;

        parseSourceList(sourceList);
    }

    private void parseSourceList(String[] sourceList) {

        for (int i = 0; i < sourceList.length; i++) {
            String element = sourceList[i];
            if (element.charAt(0) == '*') continue;

            if (element.charAt(0) == '$') continue;

            if (element.charAt(0) == ':') continue;

            System.out.println("element = " + element + ", index = " + i);
        }
        System.out.println();
        System.out.println("sourceList = " + Arrays.toString(sourceList));
    }


}
