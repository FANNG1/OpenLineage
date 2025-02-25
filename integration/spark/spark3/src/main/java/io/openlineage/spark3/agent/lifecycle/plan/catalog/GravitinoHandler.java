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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.XAttr.NameSpace;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;

public class GravitinoHandler implements CatalogHandler {

  private final DatasetNamespaceCombinedResolver namespaceResolver;

  public GravitinoHandler(OpenLineageContext context) {
    namespaceResolver = new DatasetNamespaceCombinedResolver(context.getOpenLineageConfig());
  }

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog.getClass().getName().toLowerCase().contains("gravitino");
  }

  @SneakyThrows
  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String catalogName = tableCatalog.name();
    String[] nameSpace = identifier.namespace();

    if (nameSpace == null || nameSpace.length == 0) {
      nameSpace = tableCatalog.defaultNamespace();
    }

    String name = Stream.concat(
        Stream.concat(Stream.of(catalogName), Arrays.stream(nameSpace)), Stream.of(identifier.name()))
            .collect(Collectors.joining("."));
    return new DatasetIdentifier(name, "test");
  }

  @Override
  public String getName() {
    return "gravitino";
  }
}
