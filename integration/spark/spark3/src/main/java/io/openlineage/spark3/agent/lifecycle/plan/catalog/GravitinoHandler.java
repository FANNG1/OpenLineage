/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import com.google.common.base.Preconditions;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.GravitinoUtils;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

@Slf4j
public class GravitinoHandler implements CatalogHandler {

  private static final String gravitinoCatalogClassName =
      "org.apache.gravitino.spark.connector.catalog.BaseCatalog";
  private static final String metalakeConfigKey = "spark.sql.gravitino.metalake";
  private final OpenLineageContext context;

  // Gravitino metalake name is lazy initialized because we may, couldn't get it when creating
  // Gravitino handler in Spark environment without Gravitino package.
  private String gravitinoMetalakeName;

  public GravitinoHandler(OpenLineageContext context) {
    this.context = context;
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
    return GravitinoUtils.getGravitinoDatasetIdentifier(
        getGravitinoMetalakeName(),
        tableCatalog.name(),
        tableCatalog.defaultNamespace(),
        identifier);
  }

  @Override
  public String getName() {
    return "gravitino";
  }

  private String getGravitinoMetalakeName() {
    if (gravitinoMetalakeName == null) {
      synchronized (this) {
        if (gravitinoMetalakeName != null) {
          return gravitinoMetalakeName;
        }
        gravitinoMetalakeName = context.getSparkSession().get().conf().get(metalakeConfigKey, "");
        Preconditions.checkArgument(
            StringUtils.isNotBlank(gravitinoMetalakeName),
            "Couldn't get Gravitino metalake name from configuration: " + metalakeConfigKey);
      }
    }
    return gravitinoMetalakeName;
  }
}
