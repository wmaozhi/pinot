package com.linkedin.thirdeye.dbi.dao;

import com.linkedin.thirdeye.dbi.entity.AnomalyFunction;
import com.linkedin.thirdeye.dbi.mapper.MergeConfigMapper;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;

@RegisterMapper(MergeConfigMapper.class)
public interface AnomalyFunctionDAO {

  @SqlUpdate("insert into anomaly_functions(is_active, bucket_size, bucket_unit, collection, "
      + "cron, explore_dimensions, filters, function_name, metric, metric_function, "
      + "properties, type, window_delay, window_delay_unit, window_size, window_unit) values ("
      + ":active, :bucketSize, :bucketUnit, :collection, :cron, :exploreDimensions, :filters, :functionName, :metric,"
      + ":metricFunction, :properties, :type, :windowDelay,:windowDelayUnit, :windowSize, :windowUnit)")
  @GetGeneratedKeys
  Long insert(@BindBean AnomalyFunction f);

  @SqlQuery("select * from anomaly_functions where id=:id")
  AnomalyFunction findById(Long id);

  @SqlUpdate("delete from anomaly_functions where id=:id")
  void delete(@Bind("id") Long id);
}
