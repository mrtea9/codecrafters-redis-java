import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        EventLoop eventLoop;

        int port = 6379; // default port
        String replicaOf = "";

        if (args.length >=4 && args[0].equals("--dir") && args[2].equals("--db")) {
            String dirName = args[1];
            String dbFileName = args[3];

            eventLoop = new EventLoop(dirName, dbFileName);
        } else if (args.length >= 4 && args[0].equals("--port") && args[2].equals("--replica")) {
            port = Integer.parseInt(args[1]);
            replicaOf = args[3];
            System.out.println(replicaOf);

            eventLoop = new EventLoop(port, replicaOf);
        } else if (args.length >= 2 && args[0].equals("--port")) {
            port = Integer.parseInt(args[1]);

            eventLoop = new EventLoop(port, replicaOf);
        } else {
            eventLoop = new EventLoop(port, replicaOf);
        }

        eventLoop.start();
    }
}
