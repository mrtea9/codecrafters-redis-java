import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BlockedClient {
    private final SocketChannel channel;
    private final List<String> startIds;
    private final CompletableFuture<Void> future;

    public BlockedClient(SocketChannel channel, List<String> startIds, CompletableFuture<Void> future) {
        this.channel = channel;
        this.startIds = startIds;
        this.future = future;
    }

    public SocketChannel getChannel() {
        return this.channel;
    }

    public List<String> getStartIds() {
        return this.startIds;
    }

    public CompletableFuture<Void> getFuture() {
        return this.future;
    }
}
