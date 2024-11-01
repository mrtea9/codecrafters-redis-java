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

            System.out.println("NIO Server listening on port 6379...");

            while (true) {
                selector.select(); // Block until at least one channel is ready
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();

                    if (key.isAcceptable()) {
                        SocketChannel clientChannel = serverSocketChannel.accept(); // Accepts the connection
                        if (clientChannel != null) { // Check if clientChannel is not null
                            System.out.println("Client connected: " + clientChannel.getRemoteAddress());
                            clientChannel.configureBlocking(false); // Set client channel to non-blocking
                            clientChannel.register(selector, SelectionKey.OP_READ); // Register for read events
                        } else {
                            System.out.println("Failed to accept connection, clientChannel is null.");
                        }
                    }

                    if (key.isReadable()) {
                        // Handle reading from the client
                        SocketChannel clientChannel = (SocketChannel) key.channel();
                        handleClient(clientChannel); // Call handleClient method
                    }

                    iterator.remove(); // Remove the processed key
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(256);
            int bytesRead = clientChannel.read(buffer);

            if (bytesRead == -1) {
                // The client has closed the connection
                System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
                clientChannel.close();
                return;
            }

            // Prepare the buffer for reading
            String line = new String(buffer.array()).trim();
            System.out.println("Received data: " + line);
            // Here, you can process the received data as needed

            // Echo back the received data to the client
            clientChannel.write(ByteBuffer.wrap(("Echo: " + line + "\r\n").getBytes()));
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
