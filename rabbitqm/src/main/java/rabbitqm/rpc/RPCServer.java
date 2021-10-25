package rabbitqm.rpc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RPCServer {

  public static final String DEFAULT_EXCHANGE = "";
  public static final String RPC_QUEUE = "rpc_queue";

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      // Define the queue that the client is going to send the initial remote call.
      ch.queueDeclare(RPC_QUEUE, false, false, false, null);
      ch.queuePurge(RPC_QUEUE); // clear the queue

      ch.basicQos(1);

      Object lock = new Object();
      DeliverCallback callback = (consumerTag, delivery) -> {
        // Handle the remote call from client
        String receivedMessage = new String(delivery.getBody(), "UTF-8");
        System.out.println(" [x] Received message: " + receivedMessage);
        
        ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        
        // Return the response by pushing message onto the "replyTo" queue
        AMQP.BasicProperties pros = new AMQP.BasicProperties.Builder()
            .correlationId(delivery.getProperties().getCorrelationId()).build();
        ch.basicPublish(DEFAULT_EXCHANGE, delivery.getProperties().getReplyTo(), pros, "response".getBytes("UTF-8"));
        synchronized (lock) {
          lock.notify();
        }
      };

      // Actual consume of remote call from client
      ch.basicConsume(RPC_QUEUE, false, callback, consumerTag -> {      });
      while (true) {
        synchronized (lock) {
          try {
            System.out.println(" [x] Awating Remote Calls...");
            lock.wait();
          } catch (InterruptedException e) {
            System.err.println(e);
          }
        }
      }
    }
  }
}
