package com.linkedin.thirdeye.db.dao;

import com.google.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import com.linkedin.thirdeye.dbi.GenericResultSetMapper;
import com.linkedin.thirdeye.dbi.SqlQueryBuilder;
import javax.sql.DataSource;

public class AbstractBaseDAO<E extends AbstractBaseEntity> {

  final Class<E> entityClass;
  final GenericResultSetMapper genericResultSetMapper = new GenericResultSetMapper();

  @Inject
  SqlQueryBuilder sqlQueryBuilder;

  @Inject
  DataSource dataSource;

  /**
   * Use at your own risk!!!
   * Ensure to close the connection after using it or it can cause a leak.
   * @return
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    // ensure to close the connection
    return dataSource.getConnection();
  }

  AbstractBaseDAO(Class<E> entityClass) {
    this.entityClass = entityClass;
  }

  public Long save(E entity) {
    try (Connection connection = getConnection()) {
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

  @SuppressWarnings("unchecked")
  public E findById(Long id) {
    try (Connection connection = getConnection()) {
      PreparedStatement selectStatement =
          sqlQueryBuilder.createFindByIdStatement(connection, entityClass, id);
      ResultSet resultSet = selectStatement.executeQuery();
      return (E) genericResultSetMapper.mapSingle(resultSet, entityClass);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public int deleteById(Long id) {
    try (Connection connection = getConnection()) {
      PreparedStatement deleteStatement =
          sqlQueryBuilder.createDeleteByIdStatement(connection, entityClass, id);
      return deleteStatement.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  @SuppressWarnings("unchecked")
  public List<E> findByParams(Map<String, Object> filters) {
    try (Connection connection = getConnection()) {
      PreparedStatement selectStatement =
          sqlQueryBuilder.createFindByParamsStatement(connection, entityClass, filters);
      ResultSet resultSet = selectStatement.executeQuery();
      return (List<E>) genericResultSetMapper.mapAll(resultSet, entityClass);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public int update(E entity) {
    try (Connection connection = getConnection()) {
      PreparedStatement updateStatement =
          sqlQueryBuilder.createUpdateStatement(connection, entity, null);
      return updateStatement.executeUpdate();
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return 0;
  }

  public int update(E entity, Set<String> fieldsToUpdate) {
    try (Connection connection = getConnection()) {
      PreparedStatement updateStatement =
          sqlQueryBuilder.createUpdateStatement(connection, entity, fieldsToUpdate);
      return updateStatement.executeUpdate();
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return 0;
  }

}
