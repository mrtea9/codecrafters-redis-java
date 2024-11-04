import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.function.Function;

public class EventLoop {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private final Map<String, Function<String, String>> handlers;
    private final Deque<EventResult> processedEvents;
    private Map<String, String> globalKeys = new HashMap<>();

    EventLoop(int port) {
        this.port = port;
        this.handlers = new HashMap<>();
        this.processedEvents = new ArrayDeque<>();
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

                if (key.isReadable()) handleClient((SocketChannel) key.channel());

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

    private void handleClient(SocketChannel clientChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesRead = clientChannel.read(buffer);

        if (bytesRead == -1) {
            System.out.println("Client disconnected: " + clientChannel.getLocalAddress());
            clientChannel.close();
            return;
        }

        String line = new String(buffer.array());
        Parser parser = new Parser(line);
        parser.parse();
        List<String> decodedList = parser.getDecodedResponse();

        processResponse(decodedList, clientChannel);
    }

    private void processResponse(List<String> decodedList, SocketChannel clientChannel) throws IOException {
        String command = decodedList.get(0);
        System.out.println(decodedList);

        if (command.equalsIgnoreCase("ping")) {
            processPing(clientChannel);
        } else if (command.equalsIgnoreCase("echo")) {
            String value = decodedList.get(1);

            processEcho(clientChannel, value);
        } else if (command.equalsIgnoreCase("set")) {
            String key = decodedList.get(1);
            String value = decodedList.get(2);

            processSet(clientChannel, key, value);
        } else if (command.equalsIgnoreCase("get")) {
            processGet();
        }
    }

    private static void processPing(SocketChannel clientChannel) throws IOException {
        clientChannel.write(ByteBuffer.wrap(("+PONG\r\n").getBytes()));
    }

    private static void processEcho(SocketChannel clientChannel, String value) throws IOException {
        String response = "+" + value + "\r\n";

        clientChannel.write(ByteBuffer.wrap(response.getBytes()));
    }

    private void processSet(SocketChannel clientChannel, String key, String value) throws IOException {
        this.globalKeys.put(key, value);

        clientChannel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
    }

    private void processGet(SocketChannel clientChannel, String key, String value) throws IOException {
        String result = this.globalKeys.get(key);

        clientChannel.write(ByteBuffer.wrap(result.getBytes()));
    }
}
