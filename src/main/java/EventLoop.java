import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class EventLoop {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private final Map<String, Function<String, String>> handlers;
    private final Deque<EventResult> processedEvents;

    private Map<String, String> globalKeys;

    EventLoop(int port) {
        this.port = port;
        this.handlers = new HashMap<>();
        this.processedEvents = new ArrayDeque<>();
        this.globalKeys = new ConcurrentHashMap<>();
    }

    public void start() {
        try {
            initialize();
            runEventLoop();
        } catch (IOException e) {
            System.out.println("Error " + e.getMessage());
        }
    }

    private void initialize() throws IOException {
        this.selector = Selector.open();
        this.serverSocketChannel = ServerSocketChannel.open();
        this.serverSocketChannel.bind(new InetSocketAddress(this.port));
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void on(String key, Function<String, String> handler) {
        this.handlers.put(key, handler);
    }

    public void runEventLoop() throws IOException {

        while (true) {
            this.selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable()) acceptConnection();

                if (key.isReadable()) {
                    Client client = new Client((SocketChannel) key.channel(), this.globalKeys);
                    client.handleClient();
                    this.globalKeys = client.getKeys();
                }

                iterator.remove();
            }
        }
    }

    private void acceptConnection() throws IOException {
        SocketChannel clientChannel = this.serverSocketChannel.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(this.selector, SelectionKey.OP_READ);
        }
    }
}
