package websocket;

import com.company.ZooHelper;
import org.apache.zookeeper.ZooKeeper;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class OnlineUserKeepAlive extends WebSocketServer {

    private final static int port = 48081;
    private List<WebSocket> keepalive_list;
    private ZooKeeper zoo;
    private String keeperhost;


    public OnlineUserKeepAlive(String keeperhost){
        super(new InetSocketAddress(port));
        this.keepalive_list = new ArrayList<WebSocket>();
        this.keeperhost = keeperhost;
        try {
            this.zoo = ZooHelper.getConnection(keeperhost);
        } catch (Exception e) { e.printStackTrace(); }
    }


    public static void main(String args[]){

        String ipAddrPort = ZooHelper.validateAddressParams(args);

        OnlineUserKeepAlive watcher = new OnlineUserKeepAlive(ipAddrPort);
        System.out.println("Listening on port"+ watcher.getAddress().toString());
        watcher.execution();
    }


    public void execution(){

        List<String> onlineUsers;
        JSONObject onlineUsers_message = new JSONObject();
        System.out.println("Started the online user keep alive");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.start();

        while(true){

            try {
                onlineUsers = zoo.getChildren("/online", false);
                //onlineUsers = Arrays.asList("sup1", "sup2", "sup3");
                onlineUsers_message.put("users", onlineUsers);

                System.out.println("Sending keep alive. Number of users:" + onlineUsers.size());
                //Sending list of users
                for(WebSocket socket:keepalive_list)
                    socket.send(onlineUsers_message.toString());

                Thread.sleep(10000);

            } catch (Exception e) { e.printStackTrace(); }
        }

    }


    @Override
    public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
        if(!this.keepalive_list.contains(webSocket))
            this.keepalive_list.add(webSocket);
        else
            System.out.println("<ERROR> Already saved connection" + webSocket.getRemoteSocketAddress());
    System.out.println("Opening connect" + webSocket.getRemoteSocketAddress());

    }

    @Override
    public void onClose(WebSocket webSocket, int i, String s, boolean b) {
        if(this.keepalive_list.contains(webSocket))
            this.keepalive_list.remove(webSocket);
        System.out.println("Closing connect" + webSocket.getRemoteSocketAddress());

    }

    @Override
    public void onMessage(WebSocket webSocket, String s) {
        System.out.println("Received message from" + webSocket.getRemoteSocketAddress() + " with message" + s);
    }

    @Override
    public void onError(WebSocket webSocket, Exception e) {
        if(this.keepalive_list.contains(webSocket))
            this.keepalive_list.remove(webSocket);
        System.out.println("<ERROR> Error from socket " + webSocket + " with error" + e.getMessage());
    }
}
