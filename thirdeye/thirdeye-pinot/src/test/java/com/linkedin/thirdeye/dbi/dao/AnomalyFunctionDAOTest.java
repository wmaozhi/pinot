package com.linkedin.thirdeye.dbi.dao;

import com.linkedin.thirdeye.dbi.JdbiPersistenceUtil;
import com.linkedin.thirdeye.dbi.entity.AnomalyFunction;
import org.testng.annotations.Test;

public class AnomalyFunctionDAOTest extends AbstractDBIBase {

//  @Test
  public void createFunctionTest() {
    AnomalyFunctionDAO functionDBI = JdbiPersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    AnomalyFunction af = super.getTestAnomalyFunction();
    long id = functionDBI.insert(af);
    System.out.println(id);
  }

}
