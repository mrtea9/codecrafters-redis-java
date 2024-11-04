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
        String[] sourceList = this.source.split("\r\n");
        System.out.println("sourceList = " + Arrays.toString(sourceList));
    }

}
