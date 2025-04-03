package io.openlineage.client.utils.gravitino;

import java.util.Map;
import java.util.Optional;

public interface GravitinoInfoProvider {
  boolean isAvailable();

  boolean useGravitinoIdentifier();

  Map<String, String> getCatalogMapping();

  default Optional<String> getGravitinoCatalogName(String catalogName) {
    return Optional.empty();
  }

  Optional<String> getMetalake();
}
