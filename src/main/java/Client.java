import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Client {
    private final SocketChannel channel;
    private final Map<String, KeyValue> keys;
    private final Map<String, String> config;
    private String time;
    private final EventLoop eventLoop;
    private final Object waitLock = new Object();


    public Client(SocketChannel channel, Map<String, KeyValue> keys, Map<String, String> config, EventLoop eventLoop) {
        this.channel = channel;
        this.keys = keys;
        this.config = config;
        this.eventLoop = eventLoop;
    }

    public Map<String, KeyValue> getKeys() {
        return this.keys;
    }

    public void handleClient() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesRead = this.channel.read(buffer);

        if (bytesRead == -1) {
            System.out.println("Client disconnected: " + this.channel.getLocalAddress());
            this.channel.close();
            return;
        }

        String line = new String(buffer.array());
        Parser parser = new Parser(line);
        parser.parse();
        List<String> decodedList = parser.getDecodedResponse();

        processResponse(decodedList);
    }

    private void processResponse(List<String> decodedList) throws IOException {
        String command = decodedList.get(0);
        System.out.println("client = " + this.channel.getRemoteAddress() + " " + decodedList);
        if (command.equalsIgnoreCase("ping")) {
            processPing();
        } else if (command.equalsIgnoreCase("echo")) {
            String value = decodedList.get(1);

            processEcho(value);
        } else if (command.equalsIgnoreCase("set")) {
            this.time = "";
            String key = decodedList.get(1);
            String value = decodedList.get(2);
            if (decodedList.size() > 3) {
                this.time = decodedList.get(4);
            }

            processSet(key, value);
        } else if (command.equalsIgnoreCase("get")) {
            String key = decodedList.get(1);

            processGet(key);
        } else if (command.equalsIgnoreCase("config")) {
            String commandArg = decodedList.get(1);
            String key = decodedList.get(2);

            processConfig(key, commandArg);
        } else if (command.equalsIgnoreCase("keys")) {

            processKeys();
        } else if (command.equalsIgnoreCase("info")) {
            String commandArg = decodedList.get(1);

            processInfo();
        } else if (command.equalsIgnoreCase("replconf")) {
            String commandArg = decodedList.get(1);
            String bytes = decodedList.get(2);

            processReplconf(commandArg, bytes);
        } else if (command.equalsIgnoreCase("psync")) {
            this.eventLoop.replicaChannels.add(this.channel);

            processPsync();
        } else if (command.equalsIgnoreCase("wait")) {
            String argument = decodedList.get(1);
            String timeWait = decodedList.get(2);

            processWait(argument, timeWait);
        }
    }

    private void processPing() throws IOException {
        this.channel.write(ByteBuffer.wrap(("+PONG\r\n").getBytes()));
    }

    private void processEcho(String value) throws IOException {
        String response = "+" + value + "\r\n";

        this.channel.write(ByteBuffer.wrap(response.getBytes()));
    }

    private void processSet(String key, String value) throws IOException {
        KeyValue valueKey = new KeyValue(value, 0);

        if (this.time.isEmpty()) {
            valueKey.expiryTimestamp = 0;
        } else {
            valueKey.expiryTimestamp = System.currentTimeMillis() + Long.parseLong(this.time);
        }

        this.keys.put(key, valueKey);
        this.eventLoop.propagateCommand("SET", key, value);
        this.eventLoop.propagateCommand("REPLCONF", "GETACK", "*");

        this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
    }

    private void processGet(String key) throws IOException {
        String result = "$-1\r\n";

        KeyValue value = this.keys.get(key);
        System.out.println(System.currentTimeMillis());
        System.out.println(value.expiryTimestamp);
        if (value.expiryTimestamp > System.currentTimeMillis() || value.expiryTimestamp == 0) {
            result = Parser.encodeBulkString(value.value);
        }

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

    private void processConfig(String key, String commandArg) throws IOException {
        HashMap<String, String> inter = new HashMap<>();
        String value = this.config.get(key);
        inter.put(key, value);

        String result = Parser.encodeArray(inter);

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

    private void processKeys() throws IOException {
        String result = Parser.encodeArray(this.keys.keySet());

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

    private void processInfo() throws IOException {
        String replicaOf = this.config.get("--replicaof");
        String result = "";
        String masterReplId = "master_replid:" + this.config.get("master_replid");
        String masterReplOffset = "master_repl_offset:" + this.config.get("master_repl_offset");

        result = replicaOf.isEmpty() ? "role:master" : "role:slave";
        result += "\r\n" + masterReplOffset + "\r\n" + masterReplId;
        result = Parser.encodeBulkString(result);

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

    private void processReplconf(String commandArg, String bytes) throws IOException {
        System.out.println("acknow = " + this.eventLoop.acknowledged);
        if (commandArg.equalsIgnoreCase("listening-port")) this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
        if (commandArg.equalsIgnoreCase("capa")) this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
        if (commandArg.equalsIgnoreCase("ack")) {
            synchronized (waitLock) {
                System.out.println("Processing REPLCONF ACK command");
                this.eventLoop.acknowledged.incrementAndGet();
                System.out.println("Acknowledged incremented to: " + this.eventLoop.acknowledged.get());
                waitLock.notifyAll(); // Notify waiting WAIT command
            }
        }

    }

    private void processPsync() throws IOException {
        String response = "+FULLRESYNC " + this.config.get("master_replid") + " " + this.config.get("master_repl_offset") + "\r\n";

        this.channel.write(ByteBuffer.wrap(response.getBytes()));

        sendRdbFile();
    }

    private void processWait(String argument, String timeWait) throws IOException {
        int replicas = Integer.parseInt(argument);
        int timeout = Integer.parseInt(timeWait);

        long startTime = System.currentTimeMillis();
        System.out.println("Starting WAIT command. Required replicas: " + replicas + ", Timeout: " + timeout);

        synchronized (waitLock) {
            while (true) {
                int acknowledged = this.eventLoop.acknowledged.get();
                System.out.println("Current acknowledged count: " + acknowledged);

                if (acknowledged >= replicas) {
                    System.out.println("Required replicas acknowledged. Exiting WAIT.");
                    break;
                }

                if (System.currentTimeMillis() - startTime >= timeout) {
                    System.out.println("Timeout reached. Exiting WAIT.");
                    break;
                }

                try {
                    waitLock.wait(50); // Wait for acknowledgments or timeout
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        int finalAcknowledged = this.eventLoop.acknowledged.get();
        System.out.println("Final acknowledged count: " + finalAcknowledged);

        String response = ":" + finalAcknowledged + "\r\n";
        this.channel.write(ByteBuffer.wrap(response.getBytes()));
    }

    private void sendRdbFile() throws IOException {
        String content = "524544495330303131FA0972656469732D76657205372E322E30FA0A72656469732D62697473C040FE00FB0000FF87B1A7CD0B1FC06E";

        byte[] contents = HexFormat.of().parseHex(content);

        this.channel.write(ByteBuffer.wrap(("$" + contents.length + "\r\n").getBytes()));
        this.channel.write(ByteBuffer.wrap(contents));
    }
}
