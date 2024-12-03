package io.openlineage.client.utils.gravitino;

import java.util.Optional;

public interface GravitinoMetalakeProvider {
  boolean isAvailable();

  Optional<String> getMetalake();
}
