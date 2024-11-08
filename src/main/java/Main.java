import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        EventLoop eventLoop;

        if (args.length > 3 && !args[0].equals("--port")) {
            String dirName = args[1];
            String dbFileName = args[3];

            eventLoop = new EventLoop(dirName, dbFileName);
        } else if (args.length > 1) {
            int port = Integer.parseInt(args[1]);
            String replicaOf = args[3];
            System.out.println(replicaOf);

            eventLoop = new EventLoop(port);
        } else {
            eventLoop = new EventLoop(6379);
        }

        eventLoop.start();
    }
}
