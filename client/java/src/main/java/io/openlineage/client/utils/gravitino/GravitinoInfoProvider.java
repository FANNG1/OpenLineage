package io.openlineage.client.utils.gravitino;

public interface GravitinoInfoProvider {
  boolean isAvailable();

  GravitinoInfo getGravitinoInfo();
}
