package io.openlineage.client.utils.gravitino;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class GravitinoInfoProviderImpl {
  private String metalake;
  private List<GravitinoInfoProvider> providers = Arrays.asList(new SparkGravitinoInfoProvider());

  private static class Holder {
    private static final GravitinoInfoProviderImpl INSTANCE = new GravitinoInfoProviderImpl();
  }

  public static GravitinoInfoProviderImpl getInstance() {
    return Holder.INSTANCE;
  }

  private GravitinoInfoProviderImpl() {}

  public boolean useGravitinoIdentifier() {
    return false;
  }

  public String getGravitinoCatalog(String originCatalogName) {
    return originCatalogName;
  }

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
    for (GravitinoInfoProvider provider : providers) {
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
