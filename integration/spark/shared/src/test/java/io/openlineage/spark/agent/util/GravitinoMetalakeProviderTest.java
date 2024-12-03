package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.filesystem.gvfs.MetalakeProviderImpl;
import io.openlineage.client.utils.filesystem.gvfs.SparkGravitinoMetalakeProvider;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GravitinoMetalakeProviderTest {


  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @SneakyThrows
  @Test
  public void testSparkGravitinoMetalakeProvider() {
    MetalakeProviderImpl provider = new MetalakeProviderImpl();

    SparkSession.builder()
            .master("local[*]")
            .appName("test")
            .config(SparkGravitinoMetalakeProvider.metalakeConfigKey, "metalake_name")
            .getOrCreate();

    Assertions.assertEquals("metalake_name", provider.getMetalakeName());
  }
}
