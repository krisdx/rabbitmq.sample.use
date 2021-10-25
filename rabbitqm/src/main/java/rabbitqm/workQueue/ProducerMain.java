package rabbitqm.workQueue;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

public class ProducerMain {
  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      /**
       * Survive if the RabbitMQ crash
       */
      boolean durable = true;
      DeclareOk ok = ch.queueDeclare("workQueue", durable, false, false, null);
      Scanner sc = new Scanner(System.in);
      String message = sc.nextLine();
      while (!message.equals("exit")) {
        ch.basicPublish("", "", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + message + "'");
        message = sc.nextLine();
      }
      ch.basicPublish("", "", MessageProperties.PERSISTENT_TEXT_PLAIN, "exit".getBytes("UTF-8"));
      sc.close();
    }
  }
}