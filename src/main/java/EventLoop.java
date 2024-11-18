import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

public class EventLoop {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private final Map<String, KeyValue> globalKeys = new ConcurrentHashMap<>();
    private final Map<String, String> globalConfig = new ConcurrentHashMap<>();
    private final List<SocketChannel> replicaChannels = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public EventLoop(int port, String replicaOf) {
        this.port = port;
        this.globalConfig.put("--replicaof", replicaOf);
        this.globalConfig.put("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        this.globalConfig.put("master_repl_offset", "0");
        System.out.println("Initialized EventLoop with port: " + port + " and replicaOf: " + replicaOf);
    }

    public EventLoop(String dirName, String dbFileName) {
        this.port = 6379;
        this.globalConfig.put("dir", dirName);
        this.globalConfig.put("dbfilename", dbFileName);
        System.out.println("Initialized EventLoop with directory: " + dirName + " and DB file: " + dbFileName);
        readConfig(dirName, dbFileName);
    }

    public void start() {
        try {
            System.out.println("Starting EventLoop...");
            initialize();
            new Thread(this::connectMaster).start();
            runEventLoop();
        } catch (IOException e) {
            System.err.println("Error starting EventLoop: " + e.getMessage());
        }
    }

    private void initialize() throws IOException {
        System.out.println("Initializing selector and server socket...");
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("Server started on port " + port);
    }

    private void runEventLoop() throws IOException {
        System.out.println("Running EventLoop...");
        while (true) {
            selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();

                try {
                    if (key.isAcceptable()) {
                        System.out.println("Accepting connection...");
                        acceptConnection();
                    }
                    if (key.isReadable()) {
                        System.out.println("Handling client...");
                        handleClient(key);
                    }
                } catch (IOException e) {
                    System.err.println("Error processing client: " + e.getMessage());
                    key.cancel();
                }
            }
        }
    }

    private void acceptConnection() throws IOException {
        SocketChannel clientChannel = serverSocketChannel.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);
            System.out.println("Accepted new connection: " + clientChannel.getRemoteAddress());
        } else {
            System.out.println("No client connection to accept.");
        }
    }

    private void handleClient(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        System.out.println("Creating Client instance for: " + clientChannel.getRemoteAddress());
        Client client = new Client(clientChannel, globalKeys, globalConfig, this);
        client.handleClient();
    }

    private void readConfig(String dir, String fileName) {
        String filePath = dir + "/" + fileName;
        System.out.println("Reading config from: " + filePath);
        try (FileInputStream fin = new FileInputStream(new File(filePath))) {
            byte[] bytes = fin.readAllBytes();
            Map<String, KeyValue> database = Parser.parseRdbFile(bytes);
            globalKeys.putAll(database);
            System.out.println("Config loaded successfully.");
        } catch (Exception e) {
            System.err.println("Error reading config file: " + e.getMessage());
        }
    }

    private void connectMaster() {
        String replicaOf = globalConfig.get("--replicaof");
        if (replicaOf == null || replicaOf.isEmpty()) {
            System.out.println("No replica to connect to.");
            return;
        }

        String[] parts = replicaOf.split(" ");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        System.out.println("Attempting to connect to master at " + host + ":" + port);
        try (SocketChannel masterChannel = SocketChannel.open()) {
            masterChannel.configureBlocking(false);
            masterChannel.connect(new InetSocketAddress(host, port));
            while (!masterChannel.finishConnect()) Thread.yield();
            System.out.println("Connected to master at " + host + ":" + port);
            performHandshake(masterChannel);
        } catch (IOException e) {
            System.err.println("Error connecting to master: " + e.getMessage());
        }
    }

    private void performHandshake(SocketChannel masterChannel) throws IOException {
        System.out.println("Performing handshake with master...");
        sendCommand(masterChannel, "PING");
        sendCommand(masterChannel, "REPLCONF", "listening-port", String.valueOf(port));
        sendCommand(masterChannel, "REPLCONF", "capa", "psync2");
        sendCommand(masterChannel, "PSYNC", "?", "-1");
        System.out.println("Handshake completed.");
    }

    private void sendCommand(SocketChannel channel, String... args) throws IOException {
        System.out.println("Sending command: " + Arrays.toString(args));
        List<String> command = Arrays.asList(args);
        String encodedCommand = Parser.encodeArray(command);
        channel.write(ByteBuffer.wrap(encodedCommand.getBytes()));
    }

    public void propagateCommand(String command, String... args) {
        System.out.println("Propagating command: " + command + " with args: " + Arrays.toString(args));
        executor.submit(() -> {
            List<String> request = new ArrayList<>();
            request.add(command);
            request.addAll(Arrays.asList(args));
            String encodedCommand = Parser.encodeArray(request);

            for (SocketChannel replicaChannel : replicaChannels) {
                if (replicaChannel.isConnected()) {
                    try {
                        replicaChannel.write(ByteBuffer.wrap(encodedCommand.getBytes()));
                        System.out.println("Command propagated to replica: " + replicaChannel.getRemoteAddress());
                    } catch (IOException e) {
                        System.err.println("Error propagating to replica: " + e.getMessage());
                    }
                } else {
                    System.out.println("Replica channel not connected: " + replicaChannel);
                }
            }
        });
    }
}
