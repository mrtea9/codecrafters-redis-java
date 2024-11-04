

public class Main {
    public static void main(String[] args) {
        EventLoop eventLoop = new EventLoop(6379);
        eventLoop.start();
    }
}
