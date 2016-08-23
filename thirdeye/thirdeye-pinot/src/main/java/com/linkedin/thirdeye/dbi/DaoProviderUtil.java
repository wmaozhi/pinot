package com.linkedin.thirdeye.dbi;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import java.io.File;
import java.sql.Connection;
import javax.validation.Validation;
import org.apache.tomcat.jdbc.pool.DataSource;

public abstract class DaoProviderUtil {

  private static Injector injector;

  public static void init(File localConfigFile) {
    PersistenceConfig configuration = PersistenceUtil.createConfiguration(localConfigFile);
    DataSource dataSource = new DataSource();
    dataSource.setInitialSize(10);
    dataSource.setDefaultAutoCommit(true);
    dataSource.setMaxActive(100);
    dataSource.setUsername("sa");
    dataSource.setPassword("sa");
    dataSource.setUrl("jdbc:h2:mem:test");
    dataSource.setDriverClassName("org.h2.Driver");
    DataSourceModule dataSourceModule = new DataSourceModule(dataSource);
    injector = Guice.createInjector(dataSourceModule);
  }

  public static PersistenceConfig createConfiguration(File configFile) {
    ConfigurationFactory<PersistenceConfig> factory =
        new ConfigurationFactory<>(PersistenceConfig.class,
            Validation.buildDefaultValidatorFactory().getValidator(), Jackson.newObjectMapper(),
            "");
    PersistenceConfig configuration;
    try {
      configuration = factory.build(configFile);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return configuration;
  }

  public static <T> T getInstance(Class<T> c) {
    return injector.getInstance(c);
  }

  static class DataSourceModule extends AbstractModule {
    SqlQueryBuilder builder;
    DataSource dataSource;

    DataSourceModule(DataSource dataSource) {
      this.dataSource = dataSource;
      try (Connection conn = dataSource.getConnection()) {
        builder = new SqlQueryBuilder();
        builder.register(conn, AnomalyFeedback.class, "ANOMALY_FEEDBACK");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override protected void configure() {
    }

    @Provides javax.sql.DataSource getDataSource() {
      return dataSource;
    }

    @Provides SqlQueryBuilder getBuilder() {
      return builder;
    }
  }
}
