package websocket;

import org.java_websocket.WebSocket;
import java.io.IOException;

public class KafkaWorkerFactory extends WorkerFactory{

    public KafkaWorkerFactory() throws IOException, InterruptedException {
        super("localhost:2181");
    }

    @Override
    protected SocketConnectedWorker create_worker(String id, WebSocket websocket){
        SocketConnectedWorker new_worker = null;
        try {
            new_worker = new KafkaSocketConnectedWorker(id, websocket, "localhost:2181", "localhost:9092");
            ((KafkaSocketConnectedWorker)new_worker).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new_worker;
    }
}
