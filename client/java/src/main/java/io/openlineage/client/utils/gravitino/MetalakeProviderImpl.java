package io.openlineage.client.utils.gravitino;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class MetalakeProviderImpl {
  private String metalake;
  private List<GravitinoMetalakeProvider> providers =
      Arrays.asList(new SparkGravitinoMetalakeProvider());

  public String getMetalakeName() {
    if (metalake != null) return metalake;
    synchronized (this) {
      if (metalake != null) {
        return metalake;
      }
      metalake = doGetMetalakeName();
    }
    return metalake;
  }

  private String doGetMetalakeName() {
    for (GravitinoMetalakeProvider provider : providers) {
      if (provider.isAvailable()) {
        Optional<String> metalakeOption = provider.getMetalake();
        if (metalakeOption.isPresent()) {
          return metalakeOption.get();
        }
      }
    }
    throw new IllegalStateException("Could not find Gravitino metalake name");
  }
}
