package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.GravitinoUtils;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class GravitinoJDBCHandler extends JdbcHandler {

  private GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.getInstance();

  public GravitinoJDBCHandler(OpenLineageContext context) {
    super(context);
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    String metalake = provider.getMetalakeName();
    String catalogName = provider.getGravitinoCatalog(tableCatalog.name());
    return GravitinoUtils.getGravitinoDatasetIdentifier(
        metalake, catalogName, tableCatalog.defaultNamespace(), identifier);
  }
}
