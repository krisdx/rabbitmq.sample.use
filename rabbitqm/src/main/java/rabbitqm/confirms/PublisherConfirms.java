package rabbitqm.confirms;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import com.rabbitmq.client.AMQP.Confirm.SelectOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PublisherConfirms {

  private static final String MESSAGE = "message";
  private static final int BATCH_SIZE = 50;

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory f = new ConnectionFactory();
    f.setHost("localhost");
    try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {

      SelectOk ok = ch.confirmSelect();

      /*
       * Sync confirms
       */
      ch.basicPublish("", ch.queueDeclare().getQueue(), null, MESSAGE.getBytes("UTF-8"));
      boolean confirm = ch.waitForConfirms(5000);
      System.out.println("Sync confirm from Broker:" + confirm);

      /*
       * Batch confirms
       */
      int n = 100;
      int counter = 0;
      for (int i = 0; i < n; i++) {
        ch.basicPublish("", ch.queueDeclare().getQueue(), null, MESSAGE.getBytes("UTF-8"));
        counter++;
        if (counter == BATCH_SIZE) {
          counter = 0;
          confirm = ch.waitForConfirms(5000);
          System.out.println("Batched [" + BATCH_SIZE + "] confirm from Broker:" + confirm);
        }
      }

      /*
       * Async confirms
       */
      ConcurrentNavigableMap<Long, String> confirms = new ConcurrentSkipListMap<>();
      ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
        if (multiple) {
          ConcurrentNavigableMap<Long, String> confirmed = confirms.headMap(sequenceNumber, true);
          confirmed.clear();
        } else {
          confirms.remove(sequenceNumber);
        }
      };
      
      ch.addConfirmListener(cleanOutstandingConfirms, new ConfirmCallback() {

        @Override
        public void handle(long sequenceNumber, boolean multiple) throws IOException {
          String body = confirms.get(sequenceNumber);
          System.err.format("Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n", body,
              sequenceNumber, multiple);
          cleanOutstandingConfirms.handle(sequenceNumber, multiple);
        }

      });

      long start = System.nanoTime();
      for (int i = 0; i < n; i++) {
        confirms.put(ch.getNextPublishSeqNo(), MESSAGE);
        ch.basicPublish("", ch.queueDeclare().getQueue(), null, MESSAGE.getBytes());
      }

      if (!waitUntil(Duration.ofSeconds(60), () -> confirms.isEmpty())) {
        throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
      }

      long end = System.nanoTime();
      System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", n,
          Duration.ofNanos(end - start).toMillis());
    }

  }

  static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
    int waited = 0;
    while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
      Thread.sleep(100L);
      waited = +100;
    }
    return condition.getAsBoolean();
  }
}
