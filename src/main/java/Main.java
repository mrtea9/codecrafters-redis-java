import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class Main {
    public static void main(String[] args) {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(6379));
            serverSocketChannel.configureBlocking(false); // Non-blocking mode
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT); // Register with selector

            while (true) {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();

                    if (key.isAcceptable()) {
                        SocketChannel clientChannel = serverSocketChannel.accept();

                        clientChannel.configureBlocking(false);
                        clientChannel.register(selector, SelectionKey.OP_READ);
                    }

                    if (key.isReadable()) {
                        SocketChannel clientChannel = (SocketChannel) key.channel();
                        handleClient(clientChannel);
                    }

                    iterator.remove();
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(256);
            int bytesRead = clientChannel.read(buffer);

            if (bytesRead == -1) {
                System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
                clientChannel.close();
                return;
            }

            String line = new String(buffer.array());
            String[] splitLine = line.split("\r\n");

            System.out.println(line);
            System.out.println("command = " + splitLine[2]);
            System.out.println("word = " + splitLine[4]);
            System.out.println("test = " + Arrays.toString(splitLine));
            // Here, you can process the received data as needed


            clientChannel.write(ByteBuffer.wrap(("+PONG\r\n").getBytes()));
        } catch (IOException e) {
            System.out.println("IOException while handling client: " + e.getMessage());
            try {
                clientChannel.close();
            } catch (IOException closeEx) {
                System.out.println("IOException while closing socket: " + closeEx.getMessage());
            }
        }
    }
}
