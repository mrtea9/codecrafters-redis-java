import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EventLoop {

    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private Map<String, String> globalTimes = new ConcurrentHashMap<>();
    private Map<String, String> globalKeys = new ConcurrentHashMap<>();

    EventLoop() {

    }

    EventLoop(String dirName, String dbFileName) {
        this.globalKeys.put("dir", dirName);
        this.globalTimes.put("dir", "0:0");

        this.globalKeys.put("dbfilename", dbFileName);
        this.globalTimes.put("dbfilename", "0:0");

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
        int port = 6379;
        this.serverSocketChannel.bind(new InetSocketAddress(port));
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void runEventLoop() throws IOException {

        while (true) {
            this.selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable()) acceptConnection();

                if (key.isReadable()) {
                    Client client = new Client((SocketChannel) key.channel(), this.globalKeys, this.globalTimes);
                    client.handleClient();
                    this.globalKeys = client.getKeys();
                    this.globalTimes = client.getTimes();
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

        try {
            File file = new File(filePath);
            byte[] bytes = new byte[(int) file.length()];

            FileInputStream fin = new FileInputStream(file);
            fin.read(bytes);

            Parser.parseRdbFile(bytes);

            fin.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
