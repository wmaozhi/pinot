package com.linkedin.thirdeye.dbi.mapper;

import com.linkedin.thirdeye.dbi.entity.AnomalyFunction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class AnomalyFunctionMapper implements ResultSetMapper<AnomalyFunction> {
  @Override public AnomalyFunction map(int index, ResultSet rs, StatementContext ctx)
      throws SQLException {
    return new AnomalyFunction(rs.getLong("id"), rs.getBoolean("is_active"),
        rs.getInt("bucket_size"), TimeUnit.valueOf(rs.getString("bucket_unit")),
        rs.getString("collection"), rs.getString("cron"), rs.getString("explore_dimensions"),
        rs.getString("filters"), rs.getString("function_name"), rs.getString("metric"),
        rs.getString("metric_function"), rs.getString("properties"), rs.getString("type"),
        rs.getInt("window_delay"), TimeUnit.valueOf(rs.getString("window_delay_unit")),
        rs.getInt("window_size"), TimeUnit.valueOf(rs.getString("window_unit")));
  }
}
