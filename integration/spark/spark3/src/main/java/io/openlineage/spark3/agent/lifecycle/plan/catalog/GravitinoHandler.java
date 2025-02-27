/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.XAttr.NameSpace;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;

@Slf4j
public class GravitinoHandler implements CatalogHandler {
  private final String gravitinoMetalakeName;
  private final String gravitinoCatalogClassName =
      "org.apache.gravitino.spark.connector.catalog.BaseCatalog";

  public GravitinoHandler(OpenLineageContext context) {
    this.gravitinoMetalakeName = context.getSparkSession().get().conf().get("spark.sql.gravitino.metalake", "null");
  }

  @Override
  public boolean hasClasses() {
    try {
      GravitinoHandler.class.getClassLoader().loadClass(gravitinoCatalogClassName);
      return true;
    } catch (Exception e) {
      log.debug("The Gravitino catalog is not present");
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof BaseCatalog;
  }

  @SneakyThrows
  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String gravitinoCatalogName = tableCatalog.name();
    String[] gravitinoNameSpace = identifier.namespace();

    if (gravitinoNameSpace == null || gravitinoNameSpace.length == 0) {
      gravitinoNameSpace = tableCatalog.defaultNamespace();
    }

    String name = Stream.concat(
        Stream.concat(Stream.of(gravitinoCatalogName), Arrays.stream(gravitinoNameSpace)), Stream.of(identifier.name()))
            .collect(Collectors.joining("."));
    return new DatasetIdentifier(name, gravitinoMetalakeName);
  }

  @Override
  public String getName() {
    return "gravitino";
  }
}
