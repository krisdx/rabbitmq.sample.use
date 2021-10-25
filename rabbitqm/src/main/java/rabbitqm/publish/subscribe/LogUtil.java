package rabbitqm.publish.subscribe;

import java.util.Random;

public class LogUtil {
  
  public static final String EXCHANGE_NAME = "logs";
  
  public static final String ROOT_CAT = "ROOT";
  public static final String FUNCTIONALITY_CAT = ROOT_CAT + "." + "FUNCTIONALITY";
  public static final String SUB_FUNCTIONALITY_CAT = FUNCTIONALITY_CAT + "." + "SUB";
  
  public static String randomLogCategory() {
    final String[] seveirty = new String[] {ROOT_CAT, FUNCTIONALITY_CAT, SUB_FUNCTIONALITY_CAT};
    Random r = new Random();
    int index = r.nextInt(seveirty.length);
    return seveirty[index];
  }
}
