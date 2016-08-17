package com.linkedin.thirdeye.dbi;

import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;
import java.io.File;
import java.sql.SQLException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public abstract class JdbiPersistenceUtil {

  private static DBI jdbi;

  public static void init(Environment environment, File localConfigFile) {
    PersistenceConfig configuration = PersistenceUtil.createConfiguration(localConfigFile);

    DataSourceFactory dbConfig = new DataSourceFactory();
    dbConfig.setUrl(configuration.getDatabaseConfiguration().getUrl());
    dbConfig.setUser(configuration.getDatabaseConfiguration().getUser());
    dbConfig.setPassword(configuration.getDatabaseConfiguration().getPassword());

    // TODO: set other props : connection pool etc

    DBIFactory factory = new DBIFactory();
    jdbi = factory.build(environment, dbConfig, "mysql");
  }

  public static <T> T getInstance(Class<T> c) {
    if (jdbi == null) {
      throw new IllegalArgumentException("Please run init first, JDBI not instantiated");
    }
    return jdbi.onDemand(c);
  }

  /**
   * Return a handle suitable for use in a transaction operation, i.e. with autoCommit = false.
   */
  public static Handle getTxHandle() {
    Handle handle = jdbi.open();
    try {
      handle.getConnection().setAutoCommit(false);
    } catch (SQLException e) {
      throw new IllegalStateException("Caught an SQLException. errorCode=" + e.getErrorCode());
    }
    return handle;
  }
}
