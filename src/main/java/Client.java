import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {
    private final SocketChannel channel;
    private final Map<String, String> keys;
    private final Map<String, Long> times;

    public Client(SocketChannel channel, Map<String, String> keys, Map<String, Long> times) {
        this.channel = channel;
        this.keys = keys;
        this.times = times;
    }

    public Map<String, String> getKeys() {
        return this.keys;
    }

    public Map<String, Long> getTimes() {
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
            String time = "";
            String key = decodedList.get(1);
            String value = decodedList.get(2);
            if (decodedList.size() > 3) {
                time = decodedList.get(4);
            }

            processSet(key, value, time);
        } else if (command.equalsIgnoreCase("get")) {
            String key = decodedList.get(1);
            processGet(key);
        }
    }

    private void processPing() throws IOException {
        this.channel.write(ByteBuffer.wrap(("+PONG\r\n").getBytes()));
    }

    private void processEcho(String value) throws IOException {
        String response = "+" + value + "\r\n";

        this.channel.write(ByteBuffer.wrap(response.getBytes()));
    }

    private void processSet(String key, String value, String time) throws IOException {
        this.keys.put(key, value);
        this.times.put(key, System.currentTimeMillis());
        Long created_on = this.times.get(key);
        System.out.println("created on = " + created_on);

        this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
    }

    private void processGet(String key) throws IOException {
        String result = "$-1\r\n";
        String value = this.keys.get(key);
        Long created_on = this.times.get(key);
        System.out.println("value = " + value);
        System.out.println("created on = " + created_on);
        System.out.println("get on = " + System.currentTimeMillis());
        System.out.println(System.currentTimeMillis() - created_on);
        if (value != null) result = "$" + value.length() + "\r\n" + value + "\r\n";

        this.channel.write(ByteBuffer.wrap(result.getBytes()));
    }

}
