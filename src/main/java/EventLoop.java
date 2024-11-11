import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EventLoop {

    private final int port;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private Map<String, KeyValue> globalKeys = new ConcurrentHashMap<>();
    private final Map<String, String> globalConfig = new ConcurrentHashMap<>();

    EventLoop(int port, String replicaOf) {
        this.port = port;
        this.globalConfig.put("--replicaof", replicaOf);
    }

    EventLoop(String dirName, String dbFileName) {
        this.port = 6379;
        this.globalConfig.put("dir", dirName);
        this.globalConfig.put("dbfilename", dbFileName);

        readConfig(dirName, dbFileName);
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

        connectMaster();
    }

    public void runEventLoop() throws IOException {

        while (true) {
            this.selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable()) acceptConnection();

                if (key.isReadable()) {
                    Client client = new Client((SocketChannel) key.channel(), this.globalKeys, this.globalConfig);
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
        sendPing(masterChannel);
        readResponse(masterChannel);

        Thread.sleep(10);

        sendReplConfPort(masterChannel);
        readResponse(masterChannel);

        Thread.sleep(10);

        sendReplConfCapa(masterChannel);
        readResponse(masterChannel);

        Thread.sleep(10);

        sendPsync(masterChannel);
        readResponse(masterChannel);
    }

    private void sendPing(SocketChannel masterChannel) throws IOException {
        Set<String> request = new HashSet<>();
        request.add("PING");

        masterChannel.write(ByteBuffer.wrap((Parser.encodeArray(request)).getBytes()));
    }

    private void sendReplConfPort(SocketChannel masterChannel) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("REPLCONF");
        request.add("listening-port");
        request.add(String.valueOf(this.port));

        masterChannel.write(ByteBuffer.wrap(Parser.encodeArray(request).getBytes()));
    }

    private void sendReplConfCapa(SocketChannel masterChannel) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("REPLCONF");
        request.add("capa");
        request.add("psync2");

        masterChannel.write(ByteBuffer.wrap(Parser.encodeArray(request).getBytes()));
    }

    private void sendPsync(SocketChannel masterChannel) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("PSYNC");
        request.add("?");
        request.add("-1");

        masterChannel.write(ByteBuffer.wrap(Parser.encodeArray(request).getBytes()));
    }

    private void readResponse(SocketChannel masterChannel) throws  IOException {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        int bytesRead = masterChannel.read(buffer);

        System.out.println(bytesRead);

        if (bytesRead <= 0) return;

        byte[] responseBytes = new byte[bytesRead];
        buffer.get(responseBytes);
        System.out.println("Received response from master: " + new String(responseBytes, StandardCharsets.US_ASCII));
    }
}
