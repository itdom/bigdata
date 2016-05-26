package cn.gxlx.etl.flume;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * 向flume发送数据
 * 
 * @author GDL
 *
 */
public class AvroFlume {

    public static void main(String[] args) {
        sendRpc("slave05", 4141);
        // sendThrift("slave05", 4141);
    }

    /**
     * 发送RPC
     */
    public static void sendRpc(String hostname, int port) {
        MyRpcClientFacade client = new MyRpcClientFacade(hostname, port);
        String sampleData = "gao!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }
        client.cleanUp();
    }

    /**
     * 发送RPC
     */
    public static void sendThrift(String hostname, int port) {
        MyThriftClientFacade client = new MyThriftClientFacade(hostname, port);
        String sampleData = "gao!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }
        client.cleanUp();
    }
}

/**
 * 通过Rpc发送数据到flume
 * @author GDL
 *
 */
class MyRpcClientFacade {
    private RpcClient client;
    private String hostname;
    private int port;

    public MyRpcClientFacade(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);
    }

    public void sendDataToFlume(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
            e.printStackTrace();
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}

/**
 * @author GDL
 *
 */
class MyThriftClientFacade {
    private RpcClient client;
    private String hostname;
    private int port;

    public MyThriftClientFacade(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);

    }

    public void sendDataToFlume(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
            e.printStackTrace();
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}