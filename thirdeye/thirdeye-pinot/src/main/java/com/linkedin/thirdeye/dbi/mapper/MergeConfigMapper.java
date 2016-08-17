package com.linkedin.thirdeye.dbi.mapper;

import com.linkedin.thirdeye.dbi.entity.MergeConfig;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class MergeConfigMapper implements ResultSetMapper<MergeConfig> {
  @Override public MergeConfig map(int index, ResultSet rs, StatementContext ctx)
      throws SQLException {
    return new MergeConfig(rs.getLong("id"), rs.getString("strategy"),
        rs.getLong("allowed_sequential_gap"), rs.getLong("merge_duration"),
        rs.getLong("function_id"));
  }
}
