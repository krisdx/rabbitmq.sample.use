package rabbitqm.publish.subscribe;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsumerManager {

  public static void main(String[] args) throws IOException, TimeoutException {
    startLogConsumer(new LogConsumer(LogUtil.ROOT_CAT));
    startLogConsumer(new LogConsumer(LogUtil.FUNCTIONALITY_CAT));
    startLogConsumer(new LogConsumer(LogUtil.SUB_FUNCTIONALITY_CAT));
  }

  private static void startLogConsumer(LogConsumer logConsumer) {
    new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          logConsumer.start();
        } catch (IOException | TimeoutException | InterruptedException e) {
          e.printStackTrace();
        }
      }
    }).start();
  }
}