package websocket;

import com.company.ZooHelper;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONObject;
import org.apache.zookeeper.KeeperException;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Scanner;


public class WorkerFactory extends WebSocketServer{

    private final static int port = 48080;
    private int last_worker_port;
    private String hostkeeper;
    private HashMap<String, SocketConnectedWorker> workers_map;


    public WorkerFactory(String hostkeeper) throws IOException, InterruptedException {
        super(new InetSocketAddress(port));
        this.last_worker_port = port+1;
        this.workers_map  = new HashMap<String, SocketConnectedWorker>();
        this.hostkeeper = hostkeeper;

    }

    public String getHostkeeper(){
        return this.hostkeeper;
    }

    @Override
    public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
        System.out.println("Factory: New connection from:"  + webSocket.getRemoteSocketAddress().toString());
    }

    @Override
    public void onClose(WebSocket webSocket, int i, String s, boolean b) {
        System.out.println("Factory: Closed connection from:" + webSocket.getRemoteSocketAddress().toString());
        SocketConnectedWorker worker = workers_map.remove(webSocket.getRemoteSocketAddress().toString());
        worker.disconnect();
    }


    protected SocketConnectedWorker create_worker(String id, WebSocket websocket){
        SocketConnectedWorker new_worker = null;
        try {
            new_worker = new SocketConnectedWorker(id, websocket, this.hostkeeper);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new_worker;
    }

    protected void manage_registration(WebSocket webSocket, JSONObject message){
        JSONObject reply = new JSONObject();
        if( message.get("username") == null || "".equals(message.get("username"))) {
            reply.put("status", "error");
            reply.put("additional_message", "invalid username");
        }
        else if(workers_map.containsKey(message.get("username"))){
            reply.put("type", "error");
            reply.put("additional_message", "username already online");
        }
        else {
            SocketConnectedWorker new_worker = create_worker(message.get("username").toString(),webSocket);

            if (new_worker != null && new_worker.canEnroll() && new_worker.login()) {
                reply.put("type", "registration");
                reply.put("status","registered");
                reply.put("additional_message", "new_worker.getAddress().split('/')[1]");
                workers_map.put(webSocket.getRemoteSocketAddress().toString(), new_worker);
            } else {
                reply.put("type", "error");
                reply.put("additional_message", "username already online");
            }
            webSocket.send(reply.toString()); //Sending back the reply

            System.out.println("Received and process request of user \'" + message.get("username") +  "\' with reply" + reply);
        }
    }

    @Override
    public void onMessage(WebSocket webSocket, String data) {
        JSONObject message = new JSONObject(data);
        JSONObject reply = new JSONObject();

        if(!message.has("type")) {
            webSocket.send(reply.put("type", "error").put("additional_message", "no field type, rejected").toString());
            return;
        }
        if(message.getString("type").equals("registration"))
            manage_registration(webSocket, message);
        else if("chat".equals(message.getString("type")) && message.has("dest") && message.has("sender") && message.has("msg")){
            if(workers_map.get(webSocket.getRemoteSocketAddress().toString()).writeWithAnswer(message.getString("dest"), message.getString("msg")))
                reply.put("type","confirmation");
            else
                reply.put("type","error");
            reply.put("additional_message", message.toString());
        }
        else {
            reply.put("type", "error").put("additional_message", "no required field, message was rejected");
            return;
        }
        webSocket.send(reply.toString());
        System.out.println("Received and processed request properly. Data:" +message.toString());
    }

    @Override
    public void onError(WebSocket webSocket, Exception e) {
        System.out.println("Error with socket and exception: \'" + e.getMessage() + "\'");
        //System.exit(1);
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

       String ipAddrPort = ZooHelper.validateAddressParams(args);

       final WorkerFactory factory = new WorkerFactory(ipAddrPort);

       System.out.println("Stating");

       factory.start();

       Scanner input = new Scanner(System.in);

       System.out.println("Running... enter new line to finish");

       String answer = input.nextLine();
       input.nextLine();

       factory.stop();

       System.out.println("Ending");

   }
}
