import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        EventLoop eventLoop;

        if (args.length > 1) {
            String dirName = args[1];
            String dbFileName = args[3];

            eventLoop = new EventLoop(dirName, dbFileName);
        } else {
            eventLoop = new EventLoop();
        }

        eventLoop.start();
    }
}
