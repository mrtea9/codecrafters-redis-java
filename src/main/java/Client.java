import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {
    private final SocketChannel channel;
    private final Map<String, String> keys;
    private final Map<String, String> times;
    private final Map<String, String> config;
    private String time;

    public Client(SocketChannel channel, Map<String, String> keys, Map<String, String> times, Map<String, String> config) {
        this.channel = channel;
        this.keys = keys;
        this.times = times;
        this.config = config;
    }

    public Map<String, String> getKeys() {
        return this.keys;
    }

    public Map<String, String> getTimes() {
        return this.times;
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
        System.out.println(decodedList);
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
            String value = this.keys.get(key);

            processConfig(key, commandArg);
        } else if (command.equalsIgnoreCase("keys")) {

            processKeys();
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
        this.keys.put(key, value);

        if (this.time.isEmpty()) {
            this.times.put(key, "0:0");
        } else {
            this.times.put(key, System.currentTimeMillis() + ":" + this.time);
        }

        this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
    }

    private void processGet(String key) throws IOException {
        String result = "$-1\r\n";
        String allTime = this.times.get(key);

        long createdOn = Long.parseLong(allTime.substring(0, allTime.indexOf(':')));
        String time = allTime.substring(allTime.indexOf(':') + 1);

        if (createdOn != (long) 0) {
            long timePassed = System.currentTimeMillis() - createdOn;
            if (timePassed > Long.parseLong(time)) this.keys.remove(key);
        }

        String value = this.keys.get(key);
        if (value != null) result = "$" + value.length() + "\r\n" + value + "\r\n";

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

    private void processConfig(String key, String commandArg) throws IOException {
        HashMap<String, String> inter = new HashMap<>();
        String value = this.config.get(key);
        inter.put(key, value);

        String result = Parser.encodeArray(inter);

        System.out.println("result = " + result);
        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

    private void processKeys() throws IOException {
        String result = Parser.encodeArray(this.keys.keySet());

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }
}
