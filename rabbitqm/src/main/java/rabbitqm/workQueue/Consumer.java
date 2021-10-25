package rabbitqm.workQueue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

public class Consumer {

  private volatile static String lastMessage = "";
  private volatile static int killedConsumerIndex = -1;
  private int consumerCount;

  public Consumer(int i) {
    this.consumerCount = i;
  }

  public void start() throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      int prefetchCount = 1;
      ch.basicQos(prefetchCount);

      /**
       * Survive if the RabbitMQ crash
       */
      boolean durable = true;
      DeclareOk ok = ch.queueDeclare("workQueue", durable, false, false, null);
      System.out.println("[" + consumerCount + "] Waiting for messages");

      /**
       * Delcare that we want messages to be acknowledgement before deleted from the
       * queue.
       */
      boolean autoAck = false;
      while (!lastMessage.equals("exit")) {
        ch.basicConsume(ok.getQueue(), autoAck, new DeliverCallback() {

          @Override
          public void handle(String consumerTag, Delivery message) throws IOException {

            /*
             * Testing the acknowledgements. "Kill" the first consumer so that he can't send
             * an ack, and see that the the message is coning to be sent to the next
             * consumer. Of course, acknowledgement timeout of RabbitMQ server must be
             * configured (default is 30 minutes)
             */
            if (killedConsumerIndex == -1) {
              killedConsumerIndex = consumerCount;
            }

            if (killedConsumerIndex == consumerCount) {
              System.out.println("[" + consumerCount
                  + "] Simulating consumer unable to process messages... Should be sent to next available consumer");
              return;
            }

            String received = new String(message.getBody(), "UTF-8");
            lastMessage = received;
            System.out.println("[" + consumerCount + "] Received message: " + received);

            try {
              proccessTask(received);
            } catch (Exception e) {
              System.err.println(e);
            } finally {
//              System.out.println("[" + ok.getQueue() + "] finished task: " + received);
              ch.basicAck(message.getEnvelope().getDeliveryTag(), false);
              ch.queueDelete(ok.getQueue());
            }
          }
        }, consumerTag -> {
        });
      }
    }
  }

  private void proccessTask(String task) throws InterruptedException {
    for (char ch : task.toCharArray()) {
      if (ch == '.')
        Thread.sleep(1000);
    }
  }
}