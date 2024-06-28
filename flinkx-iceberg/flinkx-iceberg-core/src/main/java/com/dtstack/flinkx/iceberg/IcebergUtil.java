package com.dtstack.flinkx.iceberg;

import com.dtstack.flinkx.iceberg.config.IcebergConfig;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.util.HashMap;
import java.util.Map;

public final class IcebergUtil {
    private IcebergUtil() {}

    public static TableLoader buildTableLoader(IcebergConfig icebergConfig) {
        Preconditions.checkNotNull(icebergConfig);
        Map<String, String> props =
                new HashMap<String, String>() {
                    {
                        put("warehouse", icebergConfig.getWarehouse());
                        put("uri", icebergConfig.getMetastoreUris());
                    }
                };
        // setup hadoop configuration
        Configuration configuration = new Configuration();
        icebergConfig.getHadoopConfig().forEach((k, v) -> configuration.set(k, (String) v));

        CatalogLoader hcl = CatalogLoader.hive(icebergConfig.getDatabase(), configuration, props);
        TableLoader tl =
                TableLoader.fromCatalog(
                        hcl,
                        TableIdentifier.of(icebergConfig.getDatabase(), icebergConfig.getTable()));
        if (tl instanceof TableLoader.CatalogTableLoader) {
            tl.open();
        }
        return tl;
    }
}
