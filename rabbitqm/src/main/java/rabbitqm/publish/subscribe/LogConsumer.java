package rabbitqm.publish.subscribe;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

public class LogConsumer {

  private volatile static String lastMessage = "";
  private final String topicKey;

  public LogConsumer(String logCategory) {
    this.topicKey = convertToTopicKey(logCategory);
  }

  private String convertToTopicKey(String logCategory) {
    if (logCategory.equals(LogUtil.ROOT_CAT)) {
      return LogUtil.ROOT_CAT + ".#";
    } else if (logCategory.equals(LogUtil.FUNCTIONALITY_CAT)) {
      String functionality = LogUtil.FUNCTIONALITY_CAT.split("\\.")[1];
      return "*." + functionality + ".#";
    } else if (logCategory.equals(LogUtil.SUB_FUNCTIONALITY_CAT)) {
      String subFunctionality = LogUtil.SUB_FUNCTIONALITY_CAT.split("\\.")[2];
      return "*.*." + subFunctionality;
    } else {
      return "#";
    }
  }

  public void start() throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      /* Declare an exchange */
      com.rabbitmq.client.AMQP.Exchange.DeclareOk exchangeOk = ch.exchangeDeclare(LogUtil.EXCHANGE_NAME, "topic");
      /* Creating a temp queue per consumer */
      String queueName = ch.queueDeclare().getQueue();
      /* But still bind them to one exchange */
      ch.queueBind(queueName, LogUtil.EXCHANGE_NAME, topicKey);

      System.out.println(topicKey + " Consumer Ready");

      while (!lastMessage.equals("exit")) {
        ch.basicConsume(queueName, true, new DeliverCallback() {

          @Override
          public void handle(String consumerTag, Delivery message) throws IOException {

            String received = new String(message.getBody(), "UTF-8");
            lastMessage = received;
            System.out.println("[" + topicKey + "] " + received);
          }
        }, consumerTag -> {
        });
      }
    }
  }
}