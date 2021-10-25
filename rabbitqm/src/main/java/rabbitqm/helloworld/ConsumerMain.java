package rabbitqm.helloworld;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;

public class ConsumerMain {

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      DeclareOk ok = ch.queueDeclare("hello_world_queue", false, false, false, null);
      System.out.println("[*] Waiting for messages");

      ch.basicConsume(ok.getQueue(), true, new DeliverCallback() {

        @Override
        public void handle(String consumerTag, Delivery message) throws IOException {
          String received = new String(message.getBody(), "UTF-8");
          System.out.println("[x] Received message: " + received);
        }
      }, new CancelCallback() {

        @Override
        public void handle(String consumerTag) throws IOException {
        }
      });
    }
  }
}