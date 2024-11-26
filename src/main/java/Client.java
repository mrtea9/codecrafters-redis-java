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
        } else if (command.equalsIgnoreCase("type")) {
            String key = decodedList.get(1);

            processType(key);
        } else if (command.equalsIgnoreCase("xadd")) {

            processXadd(decodedList);
        } else if (command.equalsIgnoreCase("xrange")) {

            processXrange(decodedList);
        } else if (command.equalsIgnoreCase("xread")) {
            decodedList.remove(0);

            processXread(decodedList);
        }
    }

    private void processXread(List<String> list) throws IOException {
        String param = list.remove(0);
        long blockTime = -1;

        if (param.equalsIgnoreCase("block")) {
            blockTime = Long.parseLong(list.remove(0));
            list.remove(0);
        }

        List<String> streamKeys = new ArrayList<>();

        while (!isValidEntryId(list.get(0))) {
            streamKeys.add(list.remove(0));
        }

        List<String> startIds = new ArrayList<>(list);

        System.out.println(streamKeys);
        System.out.println(startIds);

        if (blockTime >= 0) {
            System.out.println("wait");

            waitForEntries(streamKeys, startIds, blockTime);
        }
        else {
            List<List<String>> finalResult = fetchStreamEntries(streamKeys, startIds);

            System.out.println("xread = " + finalResult);

            String response = (finalResult.size() == 1)
                    ? Parser.encodeRead(finalResult.get(0))
                    : Parser.encodeMultipleRead(finalResult);
            writeResponse(response);
        }

//        List<List<String>> finalResult = fetchStreamEntries(streamKeys, startIds);
//
//        System.out.println("xread = " + finalResult);
//
//        String response = (finalResult.size() == 1)
//                ? Parser.encodeRead(finalResult.get(0))
//                : Parser.encodeMultipleRead(finalResult);
//
//        if (blockTime > 0) waitForEntries(streamKeys, startIds, blockTime);
//        else writeResponse(response);

       // writeResponse(response);
    }

    private List<List<String>> fetchStreamEntries(List<String> streamKeys, List<String> startIds) {
        List<List<String>> finalResult = new ArrayList<>();

        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String startRange = startIds.get(i);

            KeyValue value = this.keys.get(streamKey);
            List<String> result = new ArrayList<>();

            System.out.println("fetch = " + streamKey);
            System.out.println("fetch = " + startRange);
            System.out.println("fetch = " + value.entries);

            Iterator<Map.Entry<String, KeyValue>> iterator = value.entries.entrySet().iterator();
            boolean processing = false;

            result.add(streamKey);

            while (iterator.hasNext()) {
                Map.Entry<String, KeyValue> entry = iterator.next();
                String k = entry.getKey();
                KeyValue v = entry.getValue();

                if (isIdSmallerOrEqual(startRange, k)) processing = true;

                if (!processing) continue;

                result.add(k);
                result.add(v.key);
                result.add(v.value);

                System.out.println("key fetch = " + k + ", value key = " + v.key + ", value value = " + v.value);
            }

            finalResult.add(result);
        }

        return finalResult;
    }

    private void waitForEntries(List<String> streamKeys, List<String> startIds, long blockTime) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        for (String streamKey : streamKeys) {
            BlockedClient blockedClient = new BlockedClient(this.channel, startIds, future);
            eventLoop.registerBlockedClient(streamKey, blockedClient);
        }

        CompletableFuture<Void> timeoutFuture = CompletableFuture.runAsync(() -> {
            try {
                if (blockTime != 0) {
                    Thread.sleep(blockTime);
                } else {
                    Thread.sleep(1000);
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (!future.isDone()) {
                future.complete(null);
                try {
                    writeResponse("$-1\r\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        future.thenRun(() -> {
            try {
                System.out.println("streamKeys = " + streamKeys + "; startIds = " + startIds + " future");
                List<List<String>> finalResult = fetchStreamLast(streamKeys, startIds);
                if (!finalResult.isEmpty()) {
                    String response = Parser.encodeMultipleRead(finalResult);
                    System.out.println("final future = " + finalResult);
                    writeResponse(response);
                } else {
                    writeResponse("$-1\r\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private List<List<String>> fetchStreamLast(List<String> streamKeys, List<String> startIds) {
        List<List<String>> finalResult = new ArrayList<>();

        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String startRange = startIds.get(i);

            KeyValue value = this.keys.get(streamKey);
            List<String> result = new ArrayList<>();

            System.out.println("last = " + streamKey);
            System.out.println("last = " + startRange);
            System.out.println("last = " + value.entries);

            Iterator<Map.Entry<String, KeyValue>> iterator = value.entries.entrySet().iterator();
            boolean processing = false;

            result.add(streamKey);

            while (iterator.hasNext()) {
                Map.Entry<String, KeyValue> entry = iterator.next();
                String k = entry.getKey();
                KeyValue v = entry.getValue();

                if (isIdSmallerOrEqual(startRange, k) && !processing) {
                    processing = true;
                    continue;
                }

                if (!processing) continue;

                result.add(k);
                result.add(v.key);
                result.add(v.value);

                System.out.println("key last = " + k + ", value key = " + v.key + ", value value = " + v.value);
            }

            finalResult.add(result);
        }

        return finalResult;
    }

    private boolean isValidEntryId(String entryId) {
        if (!entryId.contains("-")) return false;

        String[] parts = entryId.split("-");

        return parts.length == 2;
    }

    private void processXrange(List<String> list) throws IOException {
        String streamKey = list.get(1);
        String startRange = list.get(2);
        String endRange = list.get(3);
        List<String> result = new ArrayList<>();
        KeyValue value = this.keys.get(streamKey);

        System.out.println(streamKey);
        System.out.println(startRange);
        System.out.println(endRange);
        System.out.println(value.entries);

        Iterator<Map.Entry<String, KeyValue>> iterator = value.entries.entrySet().iterator();
        boolean processing = startRange.equals("-");

        while (iterator.hasNext()) {
            Map.Entry<String, KeyValue> entry = iterator.next();
            String k = entry.getKey();
            KeyValue v = entry.getValue();

            if (k.equals(startRange)) processing = true;

            if (!processing) continue;

            result.add(k);
            result.add(v.key);
            result.add(v.value);

            System.out.println("key = " + k + ", value key = " + v.key + ", value value = " + v.value);

            if (k.equals(endRange)) break;
        }

        String response = Parser.encodeRange(result);
        System.out.println(response);

        writeResponse(response);
    }

    private void writeResponse(String response) throws IOException {
        this.channel.write(ByteBuffer.wrap(response.getBytes()));
    }

    private void processXadd(List<String> list) throws IOException {
        String streamKey = list.get(1);
        String rawEntryId = list.get(2);
        String key = list.get(3);
        String value = list.get(4);

        if (rawEntryId.equals("0-0")) {
            writeResponse("-ERR The ID specified in XADD must be greater than 0-0\r\n");
            return;
        }

        System.out.println(rawEntryId);
        String entryId = resolveEntryId(rawEntryId);
        System.out.println(entryId);
        System.out.println(eventLoop.minStreamId);

        if (isIdSmallerOrEqual(entryId, eventLoop.minStreamId)) {
            writeResponse("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            return;
        }

        KeyValue streamValue = this.keys.get(streamKey);
        if (streamValue == null) {
            KeyValue keyValue = new KeyValue(entryId, key, value, ValueType.STREAM);
            this.keys.put(streamKey, keyValue);
        } else {
            streamValue.addEntry(entryId, new KeyValue(key, value));
        }

        eventLoop.minStreamId = entryId;
        eventLoop.notifyBlockedClients(streamKey);

        writeResponse(Parser.encodeBulkString(entryId));

    }

    private String resolveEntryId(String rawEntryId) {
        if (!rawEntryId.contains("*")) return rawEntryId;

        String[] elements = rawEntryId.split("-");

        if (elements.length == 1) return System.currentTimeMillis() + "-0";

        int baseTime = Integer.parseInt(elements[0]);
        int sequenceNumber = (eventLoop.minStreamId.isEmpty()) ? 1 : calculateNextNumber(baseTime);

        return baseTime + "-" + sequenceNumber;
    }

    private int calculateNextNumber(int baseTime) {
        String[] minStreamIdParts = eventLoop.minStreamId.split("-");
        int minStreamTime = Integer.parseInt(minStreamIdParts[0]);
        int minStreamSequence = Integer.parseInt(minStreamIdParts[1]);

        return (baseTime == minStreamTime) ? minStreamSequence + 1 : 0;
    }

    private boolean isIdSmallerOrEqual(String id1, String id2) {
        if (id2.isEmpty()) return false;

        String[] id1Parts = id1.split("-");
        String[] id2Parts = id2.split("-");

        int id1Time = Integer.parseInt(id1Parts[0]);
        int id2Time = Integer.parseInt(id2Parts[0]);

        if (id1Time < id2Time) return true;
        if (id1Time > id2Time) return false;

        int id1Seq = Integer.parseInt(id1Parts[1]);
        int id2Seq = Integer.parseInt(id2Parts[1]);

        return id1Seq <= id2Seq;
    }

    private void processPing() throws IOException {
        writeResponse("+PONG\r\n");
    }

    private void processEcho(String value) throws IOException {
        writeResponse("+" + value + "\r\n");
    }

    private void processSet(String key, String value) throws IOException {
        KeyValue valueKey = new KeyValue(value, 0, ValueType.STRING);

        if (this.time.isEmpty()) {
            valueKey.expiryTimestamp = 0;
        } else {
            valueKey.expiryTimestamp = System.currentTimeMillis() + Long.parseLong(this.time);
        }

        this.keys.put(key, valueKey);
        this.eventLoop.propagateCommand("SET", key, value);
        this.eventLoop.noCommand = false;

        writeResponse("+OK\r\n");
    }

    private void processGet(String key) throws IOException {
        String result = "$-1\r\n";

        KeyValue value = this.keys.get(key);
        System.out.println(System.currentTimeMillis());
        System.out.println(value.expiryTimestamp);
        if (value.expiryTimestamp > System.currentTimeMillis() || value.expiryTimestamp == 0) {
            result = Parser.encodeBulkString(value.value);
        }

        writeResponse(result);
    }

    private void processConfig(String key, String commandArg) throws IOException {
        HashMap<String, String> inter = new HashMap<>();
        String value = this.config.get(key);
        inter.put(key, value);

        String result = Parser.encodeArray(inter);

        writeResponse(result);
    }

    private void processKeys() throws IOException {
        String result = Parser.encodeArray(this.keys.keySet());

        writeResponse(result);
    }

    private void processInfo() throws IOException {
        String replicaOf = this.config.get("--replicaof");
        String result = "";
        String masterReplId = "master_replid:" + this.config.get("master_replid");
        String masterReplOffset = "master_repl_offset:" + this.config.get("master_repl_offset");

        result = replicaOf.isEmpty() ? "role:master" : "role:slave";
        result += "\r\n" + masterReplOffset + "\r\n" + masterReplId;
        result = Parser.encodeBulkString(result);

        writeResponse(result);
    }

    private void processReplconf(String commandArg, String bytes) throws IOException {
        System.out.println("acknow = " + this.eventLoop.acknowledged);
        if (commandArg.equalsIgnoreCase("listening-port")) this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
        if (commandArg.equalsIgnoreCase("capa")) this.channel.write(ByteBuffer.wrap(("+OK\r\n").getBytes()));
        if (commandArg.equalsIgnoreCase("ack")) {
            System.out.println("Processing ack command");
            this.eventLoop.acknowledged.incrementAndGet();
            System.out.println("Acknowledged incremented: " + this.eventLoop.acknowledged);
            this.eventLoop.notifyAcknowledged();
        }
    }

    private void processPsync() throws IOException {
        String response = "+FULLRESYNC " + this.config.get("master_replid") + " " + this.config.get("master_repl_offset") + "\r\n";

        writeResponse(response);

        sendRdbFile();
    }

    private void processWait(String argument, String timeWait) throws IOException {
        int replicas = Integer.parseInt(argument);
        int timeout = Integer.parseInt(timeWait);

        this.eventLoop.propagateCommand("REPLCONF", "GETACK", "*");

        if (this.eventLoop.noCommand) {
            String response = ":" + this.eventLoop.replicaChannels.size() + "\r\n";
            writeResponse(response);
            return;
        }

        CompletableFuture<Integer> waitFuture = new CompletableFuture<>();
        this.eventLoop.addWaitingClient(this, waitFuture);

        // Set up a timeout to complete the future if the replicas are not reached in time
        CompletableFuture<Void> timeoutFuture = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException ignored) {
            }
            if (!waitFuture.isDone()) {
                waitFuture.complete(this.eventLoop.acknowledged.get());
            }
        });

        waitFuture.thenAccept(acknowledged -> {
            try {
                int result = Math.min(acknowledged, replicas); // Ensure we do not exceed expected replicas
                String response = ":" + result + "\r\n";

                // Reset the acknowledged count after responding
                synchronized (this.eventLoop) { // Synchronize to avoid concurrent issues
                    this.eventLoop.acknowledged.set(0);
                }

                writeResponse(response);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void sendRdbFile() throws IOException {
        String content = "524544495330303131FA0972656469732D76657205372E322E30FA0A72656469732D62697473C040FE00FB0000FF87B1A7CD0B1FC06E";

        byte[] contents = HexFormat.of().parseHex(content);

        writeResponse("$" + contents.length + "\r\n");
        this.channel.write(ByteBuffer.wrap(contents));
    }

    private void processType(String key) throws IOException {
        KeyValue value = this.keys.get(key);
        String result = "+none\r\n";
        if (value == null) {
            writeResponse(result);
            return;
        }

        ValueType type = value.type;

        if (type == ValueType.STRING) result = "+string\r\n";
        if (type == ValueType.STREAM) result = "+stream\r\n";

        writeResponse(result);
    }
}
