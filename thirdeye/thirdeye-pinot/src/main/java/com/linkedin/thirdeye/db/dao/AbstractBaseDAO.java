package com.linkedin.thirdeye.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import com.linkedin.thirdeye.dbi.GenericResultSetMapper;
import com.linkedin.thirdeye.dbi.SqlQueryBuilder;

public class AbstractBaseDAO<E extends AbstractBaseEntity> {

  final Class<E> entityClass;

  SqlQueryBuilder sqlQueryBuilder;
  
  GenericResultSetMapper genericResultSetMapper;

  Connection connection;

  AbstractBaseDAO(Class<E> entityClass) {
    this.entityClass = entityClass;
  }

  public Long save(E entity) {
    try {
      PreparedStatement insertStatement = sqlQueryBuilder.createInsertStatement(connection, entity);
      int affectedRows = insertStatement.executeUpdate();
      System.out.println("affectedRows:" + affectedRows);

      ResultSet generatedKeys = insertStatement.getGeneratedKeys();
      if (generatedKeys.next()) {
        System.out.println("Generated id:" + generatedKeys.getInt(1));
        entity.setId(generatedKeys.getLong(1));
      }
      return entity.getId();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public E findById(Long id) {
    try {
      PreparedStatement selectStatement =
          sqlQueryBuilder.createFindByIdStatement(connection, entityClass, id);
      ResultSet resultSet = selectStatement.executeQuery();
      return genericResultSetMapper.mapSingle(resultSet, entityClass);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }
}
