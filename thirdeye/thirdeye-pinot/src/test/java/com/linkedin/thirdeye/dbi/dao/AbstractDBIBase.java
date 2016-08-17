package com.linkedin.thirdeye.dbi.dao;

import com.linkedin.thirdeye.dbi.JdbiPersistenceUtil;
import com.linkedin.thirdeye.dbi.entity.AnomalyFunction;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;
import java.io.File;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;

public class AbstractDBIBase {

  @BeforeClass(alwaysRun = true) public void init() throws Exception {
    Application<Configuration> app = new Application<Configuration>() {
      @Override public void run(Configuration config, Environment environment)
          throws Exception {
        URL url = AbstractDBIBase.class.getResource("/persistence-jdbi.yml");
        File configFile = new File(url.toURI());
        JdbiPersistenceUtil.init(environment, configFile);
      }
    };
    app.run("server");
  }

  AnomalyFunction getTestAnomalyFunction() {
    return new AnomalyFunction(true, 10, TimeUnit.MINUTES, "collection", "0 1 * * *",
        "exploreDimensions", "filters", "functionName", "metric", "metricFunction", "properties",
        "type", 10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES);
  }
}
