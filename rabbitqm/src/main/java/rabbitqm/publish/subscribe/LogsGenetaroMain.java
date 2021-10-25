package rabbitqm.publish.subscribe;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class LogsGenetaroMain {
  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {
      /* Declare an exchange */
      com.rabbitmq.client.AMQP.Exchange.DeclareOk exchangeOk = ch.exchangeDeclare(LogUtil.EXCHANGE_NAME, "topic");

      Scanner sc = new Scanner(System.in);
      String message = sc.nextLine();
      while (!message.equals("exit")) {
        String randomLogCat = LogUtil.randomLogCategory();
        ch.basicPublish(LogUtil.EXCHANGE_NAME, randomLogCat, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + randomLogCat + ": " + message + "'");
        message = sc.nextLine();
      }
      /* quick hack: sending exit message to all consumers, using the sub-functionality log category */
      ch.basicPublish(LogUtil.EXCHANGE_NAME, LogUtil.SUB_FUNCTIONALITY_CAT, null, "exit".getBytes("UTF-8"));
      sc.close();
    }
  }
}