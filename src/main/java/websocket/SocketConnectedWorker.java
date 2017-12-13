package websocket;

import com.company.Worker;
import com.company.ZooHelper;
import org.apache.zookeeper.KeeperException;

import org.java_websocket.WebSocket;
import org.json.JSONObject;
import java.io.IOException;

public class SocketConnectedWorker extends Worker {

    private String id;
    private WebSocket client;

    public SocketConnectedWorker(String id, WebSocket client) throws IOException, InterruptedException {
        super(ZooHelper.getConnection(), id);
        this.id = id;
        this.client = client;
    }

    @Override
    public void read(String sender, String message) {
        JSONObject obj = new JSONObject();
        obj.put("type", "chat");
        obj.put("sender", sender);
        obj.put("dest", id);
        obj.put("message", message);
        this.client.send(obj.toString());
    }

    @Override
    public void write(String idReceiver, String message) {
        try {
            super.write(idReceiver, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean writeWithAnswer(String idReceiver, String message) {
        try {
            super.write(idReceiver, message);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    //The method enrolls return without error if the node can be enroll
    // otherwise the zookeeper throws an error and, in that case, the method returns false UNLESS the node exists error
    boolean canEnroll(){
        try {
                super.enroll();
        }
        catch (KeeperException.NodeExistsException ignore) {}
        catch (Exception e) { System.out.println(e.getMessage()); return false; }

        return true;
    }

    //Overrided for error management
    @Override
    public boolean login() {
        boolean result = true;
        try {
            result = super.login();
        } catch (Exception e) {
            return false;
        }
        return result;
    }
}
