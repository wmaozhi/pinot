package com.linkedin.thirdeye.dbi;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import com.linkedin.thirdeye.db.entity.WebappConfig;
import com.linkedin.thirdeye.dbi.entity.MergeConfig;

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
    dataSource.setUrl("jdbc:h2:~/test");
    dataSource.setDriverClassName("org.h2.Driver");
    DataSourceModule dataSourceModule = new DataSourceModule(dataSource);
    injector = Guice.createInjector(dataSourceModule);
  }

  public static PersistenceConfig createConfiguration(File configFile) {
    ConfigurationFactory<PersistenceConfig> factory = new ConfigurationFactory<>(
        PersistenceConfig.class, Validation.buildDefaultValidatorFactory().getValidator(),
        Jackson.newObjectMapper(), "");
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
    private GenericResultSetMapper genericResultSetMapper;
    EntityMappingHolder entityMappingHolder;

    DataSourceModule(DataSource dataSource) {
      this.dataSource = dataSource;
      entityMappingHolder = new EntityMappingHolder();
      try (Connection conn = dataSource.getConnection()) {
        entityMappingHolder.register(conn, AnomalyFeedback.class, "ANOMALY_FEEDBACK");
        entityMappingHolder.register(conn, AnomalyFunctionSpec.class, "ANOMALY_FUNCTIONS");
        entityMappingHolder.register(conn, AnomalyFunctionRelation.class,
            "ANOMALY_FUNCTION_RELATIONS");
        entityMappingHolder.register(conn, AnomalyJobSpec.class, "ANOMALY_JOBS");
        entityMappingHolder.register(conn, AnomalyMergedResult.class, "ANOMALY_MERGED_RESULTS");
        entityMappingHolder.register(conn, AnomalyResult.class, "ANOMALY_RESULTS");
        entityMappingHolder.register(conn, AnomalyTaskSpec.class, "ANOMALY_TASKS");
        entityMappingHolder.register(conn, EmailConfiguration.class, "EMAIL_CONFIGURATIONS");
        entityMappingHolder.register(conn, MergeConfig.class, "MERGE_CONFIG");
        entityMappingHolder.register(conn, WebappConfig.class, "WEBAPP_CONFIGS");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      builder = new SqlQueryBuilder(entityMappingHolder);

      genericResultSetMapper = new GenericResultSetMapper(entityMappingHolder);
    }

    @Override
    protected void configure() {}

    @Provides
    javax.sql.DataSource getDataSource() {
      return dataSource;
    }

    @Provides
    SqlQueryBuilder getBuilder() {
      return builder;
    }

    @Provides
    GenericResultSetMapper getResultSetMapper() {
      return genericResultSetMapper;
    }

  }
}
