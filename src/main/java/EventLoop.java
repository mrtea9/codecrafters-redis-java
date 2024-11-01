import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class EventLoop {

    private final Deque<Event> events;
    private final Map<String, Function<String, String>> handlers;
    private final Deque<EventResult> processedEvents;

    EventLoop() {
        this.events = new ArrayDeque<>();
        this.handlers = new HashMap<>();
        this.processedEvents = new ArrayDeque<>();
    }

    public void on(String key, Function<String, String> handler) {
        this.handlers.put(key, handler);
    }

    public void dispatch(Event event) {
        this.events.add(event);
    }

    public void run() {
        Event event = events.poll();

        if (event != null) {
            System.out.println("Received Event: " + event.key);
            if (this.handlers.containsKey(event.key)) {
                processEvent(event);
            }
        }

        EventResult eventResult = processedEvents.poll();

        if (eventResult != null) printEventResult(eventResult);
    }

    private void processEvent(Event event) {
        Thread eventProcessingThread = new Thread(() -> {
            creatingThread(event);
        });
        eventProcessingThread.start();
    }

    private void creatingThread(Event event) {
        Function<String, String> handler = this.handlers.get(event.key);
        String result = handler.apply(event.data);

        EventResult eventResult = new EventResult(event.key, result);

        processedEvents.add(eventResult);
    }

    private void printEventResult(EventResult eventResult) {
        System.out.println("Output for Event " + eventResult.key + " : " + eventResult.result);
    }
}
