package io.openlineage.spark3.agent.utils;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;

public class GravitinoUtils {

  public static DatasetIdentifier getGravitinoDatasetIdentifier(
      String metalake, String catalogName, String[] defaultNameSpace, Identifier identifier) {
    String[] gravitinoNameSpace = identifier.namespace();

    if (gravitinoNameSpace == null || gravitinoNameSpace.length == 0) {
      gravitinoNameSpace = defaultNameSpace;
    }

    String name =
        Stream.concat(
                Stream.concat(Stream.of(catalogName), Arrays.stream(gravitinoNameSpace)),
                Stream.of(identifier.name()))
            .collect(Collectors.joining("."));
    return new DatasetIdentifier(name, metalake);
  }
}
