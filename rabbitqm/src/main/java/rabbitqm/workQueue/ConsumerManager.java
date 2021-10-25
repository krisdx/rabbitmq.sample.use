package rabbitqm.workQueue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsumerManager {

  private static final int CONSUMERS_COUNT = 2;

  public static void main(String[] args) throws IOException, TimeoutException {
    for (int i = 0; i < CONSUMERS_COUNT; i++) {
      new Thread(new RunConsumer(i)).start();
    }
  }

  private static class RunConsumer implements Runnable {

    private int i;

    public RunConsumer(int i) {
      this.i = i;
    }

    @Override
    public void run() {
      Consumer consumer = new Consumer(i);
      try {
        consumer.start();
      } catch (IOException | TimeoutException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
