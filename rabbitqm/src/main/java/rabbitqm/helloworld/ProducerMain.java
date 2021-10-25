package rabbitqm.helloworld;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

public class ProducerMain {
  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      DeclareOk ok = ch.queueDeclare("hello_world_queue", false, false, false, null);
      String message = "Hello World";
      ch.basicPublish("", ok.getQueue(), null, message.getBytes());
      System.out.println(" [x] Sent '" + message + "'");
    }
  }
}