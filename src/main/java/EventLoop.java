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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class EventLoop {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private Map<String, KeyValue> globalKeys = new ConcurrentHashMap<>();
    private final Map<String, String> globalConfig = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Client, CompletableFuture<Integer>> waitingClients = new ConcurrentHashMap<>();
    private final Map<String, List<BlockedClient>> blockedClients = new ConcurrentHashMap<>();
    private int offset = 0;

    public List<SocketChannel> replicaChannels = new ArrayList<>();
    public AtomicInteger acknowledged = new AtomicInteger(0);
    public boolean noCommand = true;
    public Map<SocketChannel, List<List<String>>> multiCommands = new ConcurrentHashMap<>();
    public String minStreamId = "";
    public Map<SocketChannel, Boolean> multiClients = new ConcurrentHashMap<>();

    EventLoop(int port, String replicaOf) {
        this.port = port;
        this.globalConfig.put("--replicaof", replicaOf);
        System.out.println(replicaOf);
        this.globalConfig.put("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        this.globalConfig.put("master_repl_offset", "0");
    }

    EventLoop(String dirName, String dbFileName) {
        this.port = 6379;
        this.globalConfig.put("dir", dirName);
        this.globalConfig.put("dbfilename", dbFileName);

        readConfig(dirName, dbFileName);
    }

    public void registerBlockedClient(String streamKey, BlockedClient blockedClient) {
        blockedClients.computeIfAbsent(streamKey, k -> new CopyOnWriteArrayList<>()).add(blockedClient);
    }

    public void notifyBlockedClients(String streamKey) {
        List<BlockedClient> clients = blockedClients.remove(streamKey);
        if (clients == null) return;

        System.out.println("Notifying clients for stream key: " + streamKey);

        for (BlockedClient client : clients) {
            client.getFuture().complete(null);
        }
    }

    public void addWaitingClient(Client client, CompletableFuture<Integer> future) {
        this.waitingClients.put(client, future);
    }

    public void notifyAcknowledged() {
        for (Map.Entry<Client, CompletableFuture<Integer>> entry : waitingClients.entrySet()) {
            CompletableFuture<Integer> future = entry.getValue();
            if (future != null && future.isDone()) {
                future.complete(acknowledged.get());
            }
        }
    }

    public void start() {
        try {
            initialize();

            Thread connectMasterThread = new Thread(this::connectMaster);
            connectMasterThread.start();

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
        System.out.println("Server started on port " + port);
    }

    public void runEventLoop() throws IOException {

        while (true) {
            this.selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable()) acceptConnection();

                if (key.isReadable()) {
                    System.out.println("Handling client...");
                    Client client = new Client((SocketChannel) key.channel(), this.globalKeys, this.globalConfig, this);
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

    private void readConfig(String dir, String fileName) {
        String filePath = dir + "/" + fileName;
        HashMap<String, KeyValue> database;

        try {
            File file = new File(filePath);
            byte[] bytes = new byte[(int) file.length()];

            FileInputStream fin = new FileInputStream(file);
            fin.read(bytes);

            database = Parser.parseRdbFile(bytes);
            this.globalKeys.putAll(database);

            fin.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void connectMaster() {
        String replicaOf = this.globalConfig.get("--replicaof");

        if (replicaOf == null || replicaOf.isEmpty()) return;

        String[] parts = replicaOf.split(" ");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        System.out.println("address: " + host + "; port = " + port);

        try {
            SocketChannel masterChannel = SocketChannel.open();
            masterChannel.configureBlocking(false);
            masterChannel.connect(new InetSocketAddress(host, port));
            masterChannel.register(this.selector, SelectionKey.OP_CONNECT);

            while (!masterChannel.finishConnect()) {
                continue;
            }

            System.out.println("Connected to master: " + replicaOf);

            sendHandshake(masterChannel);
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
        }

    }

    private void sendHandshake(SocketChannel masterChannel) throws IOException, InterruptedException {
        sendAwaitResponse(masterChannel, this::sendPing);
        sendAwaitResponse(masterChannel, this::sendReplConfPort);
        sendAwaitResponse(masterChannel, this::sendReplConfCapa);
        sendAwaitResponse(masterChannel, this::sendPsync);
    }

    private void sendAwaitResponse(SocketChannel masterChannel, Consumer<SocketChannel> sendCommand) throws IOException {
        sendCommand.accept(masterChannel);
        processContinuousResponse(masterChannel);
    }

    private void processContinuousResponse(SocketChannel masterChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Selector readSelector = Selector.open();
        masterChannel.register(readSelector, SelectionKey.OP_READ);

        StringBuilder responseAccumulator = new StringBuilder();

        while (masterChannel.isOpen()) {
            if (readSelector.select(1100) <= 0) return;

            Iterator<SelectionKey> iterator = readSelector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();

                if (!key.isReadable()) continue;

                buffer.clear();
                int bytesRead = masterChannel.read(buffer);

                if (bytesRead <= 0) continue;

                buffer.flip();
                String responseChunk = new String(buffer.array(), 0, bytesRead);
                responseAccumulator.append(responseChunk);

                // Process complete responses
                parseAndProcessResponse(responseAccumulator, masterChannel);
            }
        }
    }

    private void parseAndProcessResponse(StringBuilder responseAccumulator, SocketChannel masterChannel) throws IOException {
        // Split the accumulated response into RESP messages
        String[] responses = responseAccumulator.toString().split("\r\n");

        List<String> responsesList = Parser.decodeArray(responses);
        System.out.println(responsesList);

        while (!responsesList.isEmpty()) {
            String firstElement = responsesList.remove(0);

            System.out.println("first = " + firstElement);

            if (firstElement.equalsIgnoreCase("set")) performSet(responsesList);
            if (firstElement.equalsIgnoreCase("ping")) performPing();
            if (firstElement.equalsIgnoreCase("replconf")) performReplConf(responsesList, masterChannel);
        }
        // Clear the accumulator if all messages were processed
        responseAccumulator.setLength(0);
    }

    private void performPing() {
        this.offset += 14;
    }

    private void performReplConf(List<String> list, SocketChannel masterChannel) throws IOException {
        String element = list.remove(0);
        List<String> request = new ArrayList<>();

        System.out.println(element);
        if (!element.equalsIgnoreCase("getack")) return;

        request.add("REPLCONF");
        request.add("ACK");
        request.add(Integer.toString(this.offset));
        System.out.println(request);

        masterChannel.write(ByteBuffer.wrap(Parser.encodeArray(request).getBytes()));
        this.offset += 37;
    }

    private void performSet(List<String> list) {
        String key = list.remove(0);
        KeyValue value = new KeyValue(list.remove(0), 0, ValueType.STRING);

        this.globalKeys.put(key, value);

        this.offset += Parser.encodeArray(Arrays.asList("SET", key, value.value)).getBytes().length;

        System.out.println("key = " + key + "; value = " + value);
    }

    private void sendPing(SocketChannel masterChannel) {
        Set<String> request = new HashSet<>();
        request.add("PING");

        try {
            masterChannel.write(ByteBuffer.wrap((Parser.encodeArray(request)).getBytes()));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void sendReplConfPort(SocketChannel masterChannel) {
        List<String> request = new ArrayList<>();
        request.add("REPLCONF");
        request.add("listening-port");
        request.add(String.valueOf(this.port));

        try {
            masterChannel.write(ByteBuffer.wrap((Parser.encodeArray(request)).getBytes()));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void sendReplConfCapa(SocketChannel masterChannel) {
        List<String> request = new ArrayList<>();
        request.add("REPLCONF");
        request.add("capa");
        request.add("psync2");

        try {
            masterChannel.write(ByteBuffer.wrap((Parser.encodeArray(request)).getBytes()));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void sendPsync(SocketChannel masterChannel) {
        List<String> request = new ArrayList<>();
        request.add("PSYNC");
        request.add("?");
        request.add("-1");

        try {
            masterChannel.write(ByteBuffer.wrap((Parser.encodeArray(request)).getBytes()));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void propagateCommand(String command, String... args) {
        if (this.replicaChannels.isEmpty()) return;

        System.out.println("Propagating command: " + command + " with args: " + Arrays.toString(args));
        List<String> request = new ArrayList<>();
        request.add(command);
        request.addAll(Arrays.asList(args));
        String encodedCommand = Parser.encodeArray(request);

        for (SocketChannel replicaChannel : this.replicaChannels) {
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
    }
}
