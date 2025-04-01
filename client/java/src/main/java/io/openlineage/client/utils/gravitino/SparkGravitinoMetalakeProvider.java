package io.openlineage.client.utils.gravitino;

import java.lang.reflect.Method;
import java.util.Optional;

public class SparkGravitinoMetalakeProvider implements GravitinoMetalakeProvider {

  private static final String SPARK_SESSION_CLASS_NAME = "org.apache.spark.sql.SparkSession";
  private static final String SPARK_RUN_CONFIG_CLASS_NAME = "org.apache.spark.sql.RuntimeConfig";
  public static final String metalakeConfigKey = "spark.hadoop.fs.gravitino.client.metalake";

  @Override
  public boolean isAvailable() {
    try {
      SparkGravitinoMetalakeProvider.class.getClassLoader().loadClass(SPARK_SESSION_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @Override
  public Optional<String> getMetalake() {
    try {
      return Optional.ofNullable(tryGetMetalake());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private String tryGetMetalake() throws Exception {

    Class<?> sparkSessionClass = Class.forName(SPARK_SESSION_CLASS_NAME);

    // SparkSession s = SparkSession.active()
    Method activeMethod = sparkSessionClass.getMethod("active");
    Object sparkSessionInstance = activeMethod.invoke(null);

    // RuntimeConfig config = s.conf()
    Method confMethod = sparkSessionClass.getMethod("conf");
    Object sparkConfInstance = confMethod.invoke(sparkSessionInstance);

    // config.get(metalakeConfigKey, null)
    Class<?> runConfigClass = Class.forName(SPARK_RUN_CONFIG_CLASS_NAME);
    Method getMethod = runConfigClass.getMethod("get", String.class, String.class);
    String metalakeName = (String) getMethod.invoke(sparkConfInstance, metalakeConfigKey, null);

    return metalakeName;
  }
}
