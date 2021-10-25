package rabbitqm.rpc;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RPCClient {

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {
      remoteCall(ch, "request");
    }
  }

  private static String remoteCall(Channel ch, String message) throws IOException, InterruptedException {
    final String correlationId = Instant.now().toString();

    // Define the queue that the server is going to send the response to
    String responseQueueName = ch.queueDeclare().getQueue();
    // Since pushing to the default Exchange, so routing key must be equal to the
    // queue name
    String routingKey = RPCServer.RPC_QUEUE;
    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(correlationId)
        .replyTo(responseQueueName).build();

    // Actual remote call...
    ch.basicPublish(RPCServer.DEFAULT_EXCHANGE, routingKey, props, message.getBytes("UTF-8"));
    System.out.println("[Client] Sent message: " + message);

    BlockingQueue<String> blockQueue = new ArrayBlockingQueue<>(1);
    // Consume the response from the server
    String consmerTag = ch.basicConsume(responseQueueName, true, (consumerTag, delivery) -> {
      if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
        blockQueue.offer(new String(delivery.getBody(), "UTF-8"));
      }
    }, consumerTag -> { });

    String response = blockQueue.take();
    System.out.println("[Client] Received message: " + response);
    ch.basicCancel(consmerTag);
    return response;
  }
}