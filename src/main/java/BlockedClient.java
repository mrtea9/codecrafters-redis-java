import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BlockedClient {
    private final SocketChannel channel;
    private final CompletableFuture<Void> future;

    public BlockedClient(SocketChannel channel, CompletableFuture<Void> future) {
        this.channel = channel;
        this.future = future;
    }

    public SocketChannel getChannel() {
        return this.channel;
    }

    public CompletableFuture<Void> getFuture() {
        return this.future;
    }
}
