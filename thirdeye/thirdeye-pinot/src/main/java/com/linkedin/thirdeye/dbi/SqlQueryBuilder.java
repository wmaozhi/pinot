package com.linkedin.thirdeye.dbi;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2.jdbcx.JdbcDataSource;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.mysql.jdbc.Statement;


public class SqlQueryBuilder {

  //Map<TableName,EntityName>
  BiMap<String, String> tableToEntityNameMap = HashBiMap.create();
  Map<String, LinkedHashMap<String, ColumnInfo>> columnInfoPerTable = new HashMap<>();
  //DB NAME to ENTITY NAME mapping
  Map<String, BiMap<String, String>> columnMappingPerTable = new HashMap<>();
  //insert sql per table
  Map<String, String> insertSqlMap = new HashMap<>();

  public void register(Connection connection, Class<? extends AbstractBaseEntity> entityClass,
      String tableName) throws Exception {
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    String catalog = null;
    String schemaPattern = null;
    String tableNamePattern = tableName;
    String columnNamePattern = null;
    ResultSet rs =
        databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

    tableToEntityNameMap.put(tableName, entityClass.getSimpleName());
    columnMappingPerTable.put(tableName, HashBiMap.create());
    LinkedHashMap<String, ColumnInfo> columnInfoMap = new LinkedHashMap<>();

    while (rs.next()) {
      String columnName = rs.getString(4);
      ColumnInfo columnInfo = new ColumnInfo();
      columnInfo.columnNameInDB = columnName;
      columnInfo.sqlType = rs.getInt(5);
      columnInfoMap.put(columnName, columnInfo);
    }
    List<Field> fields = new ArrayList<>();
    getAllFields(fields, entityClass);
    for (Field field : fields) {
      field.setAccessible(true);
      String entityColumn = field.getName();
      for (String dbColumn : columnInfoMap.keySet()) {
        boolean success = false;
        if (dbColumn.toLowerCase().equals(entityColumn.toLowerCase())) {
          success = true;
        }
        String dbColumnNormalized = dbColumn.replaceAll("_", "").toLowerCase();
        String entityColumnNormalized = entityColumn.replaceAll("_", "").toLowerCase();
        if (dbColumnNormalized.equals(entityColumnNormalized)) {
          success = true;
        }
        if (success) {
          columnInfoMap.get(dbColumn).columnNameInEntity = entityColumn;
          columnInfoMap.get(dbColumn).field = field;
          System.out.println("Mapped " + dbColumn + " to " + entityColumn);
          columnMappingPerTable.get(tableName).put(dbColumn, entityColumn);
        }
      }
    }
    columnInfoPerTable.put(tableName, columnInfoMap);
    //create insert sql for this table

    String insertSql = generateInsertSql(tableName, columnInfoMap);
    insertSqlMap.put(tableName, insertSql);
    System.out.println(insertSql);
  }

  public static String generateInsertSql(String tableName,
      LinkedHashMap<String, ColumnInfo> columnInfoMap) {

    StringBuilder values = new StringBuilder(" VALUES");
    StringBuilder names = new StringBuilder("");
    names.append("(");
    values.append("(");
    String delim = "";
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnName = columnInfo.columnNameInDB;
      if (!columnName.toLowerCase().equals("id")) {
        names.append(delim);
        names.append(columnName);
        values.append(delim);
        values.append("?");
        delim = ",";
      }
    }
    names.append(")");
    values.append(")");

    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(tableName).append(names.toString()).append(values.toString());
    return sb.toString();
  }

  public PreparedStatement createInsertStatement(Connection conn, AbstractBaseEntity entity)
      throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    return createInsertStatement(conn, tableName, entity);
  }

  public PreparedStatement createInsertStatement(Connection conn, String tableName,
      AbstractBaseEntity entity) throws Exception {

    String sql = insertSqlMap.get(tableName);
    PreparedStatement preparedStatement =
        conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    LinkedHashMap<String, ColumnInfo> columnInfoMap = columnInfoPerTable.get(tableName);
    int parameterIndex = 1;
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      if (!columnInfo.columnNameInDB.toLowerCase().equals("id")) {
        Object val = columnInfo.field.get(entity);
        System.out.println("Setting value:" + val + " for " + columnInfo.columnNameInDB);
        preparedStatement.setObject(parameterIndex++, val.toString(), columnInfo.sqlType);
      }
    }
    return preparedStatement;
  }

  public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
    fields.addAll(Arrays.asList(type.getDeclaredFields()));

    if (type.getSuperclass() != null) {
      fields = getAllFields(fields, type.getSuperclass());
    }

    return fields;
  }

  public PreparedStatement createFindByIdStatement(Connection connection,
      Class<? extends AbstractBaseEntity> entityClass, Long id) throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Select * from " + tableName + " where id=?";
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    prepareStatement.setLong(1, id);
    return prepareStatement;
  }

  public PreparedStatement createFindByParamsStatement(Connection connection,
      Class<? extends AbstractBaseEntity> entityClass, Map<String, Object> filters)
          throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    BiMap<String, String> entityNameToDBNameMapping =
        columnMappingPerTable.get(tableName).inverse();
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM " + tableName);
    StringBuilder whereClause = new StringBuilder(" WHERE ");
    LinkedHashMap<String, Object> parametersMap = new LinkedHashMap<>();
    for (String columnName : filters.keySet()) {
      String dbFieldName = entityNameToDBNameMapping.get(columnName);
      whereClause.append(dbFieldName).append("=").append("?");
      parametersMap.put(dbFieldName, filters.get(columnName));
    }
    sqlBuilder.append(whereClause.toString());
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    int parameterIndex = 1;
    LinkedHashMap<String, ColumnInfo> columnInfoMap = columnInfoPerTable.get(tableName);
    for (Entry<String, Object> paramEntry : parametersMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }

  public PreparedStatement createUpdateStatement(Connection connection, AbstractBaseEntity entity,
      Set<String> fieldsToUpdate) throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entity.getClass().getSimpleName());
    LinkedHashMap<String, ColumnInfo> columnInfoMap = columnInfoPerTable.get(tableName);

    StringBuilder sqlBuilder = new StringBuilder("UPDATE " + tableName + " SET ");
    String delim = "";
    LinkedHashMap<String, Object> parameterMap = new LinkedHashMap<>();
    for (ColumnInfo columnInfo : columnInfoMap.values()) {
      String columnNameInDB = columnInfo.columnNameInDB;
      if (!columnNameInDB.toLowerCase().equals("id")
          && (fieldsToUpdate == null || fieldsToUpdate.contains(columnInfo.columnNameInEntity))) {
        Object val = columnInfo.field.get(entity);
        if (val != null) {
          if (Enum.class.isAssignableFrom(val.getClass())) {
            val = val.toString();
          }
          sqlBuilder.append(delim);
          sqlBuilder.append(columnNameInDB);
          sqlBuilder.append("=");
          sqlBuilder.append("?");
          delim = ",";
          System.out.println("Setting value:" + val + " for " + columnInfo.columnNameInDB);
          parameterMap.put(columnNameInDB, val);
        }
      }
    }
    int parameterIndex = 1;
    PreparedStatement prepareStatement = connection.prepareStatement(sqlBuilder.toString());
    for (Entry<String, Object> paramEntry : parameterMap.entrySet()) {
      String dbFieldName = paramEntry.getKey();
      ColumnInfo info = columnInfoMap.get(dbFieldName);
      prepareStatement.setObject(parameterIndex++, paramEntry.getValue(), info.sqlType);
    }
    return prepareStatement;
  }

  public PreparedStatement createDeleteByIdStatement(Connection connection,
      Class<? extends AbstractBaseEntity> entityClass, Long id) throws Exception {
    String tableName = tableToEntityNameMap.inverse().get(entityClass.getSimpleName());
    String sql = "Delete from " + tableName + " where id=?";
    PreparedStatement prepareStatement = connection.prepareStatement(sql);
    prepareStatement.setLong(1, id);
    return prepareStatement;
  }

  class ColumnInfo {
    String columnNameInDB;
    int sqlType;
    String columnNameInEntity;
    Field field;
  }

  public static void main(String[] args) throws Exception {
    JdbcDataSource ds = new JdbcDataSource();
    ds.setURL("jdbc:h2:mem:test");
    ds.setUser("sa");
    ds.setPassword("sa");
    Connection conn = ds.getConnection();
    conn.setAutoCommit(true);
    conn.createStatement().execute(
        "Create table anomaly_feedback (id bigint, feedback_type varchar(150), comment varchar(500), status varchar(200))");
    ResultSet resultSet = conn.getMetaData().getTables(null, null, null, null);
    while (resultSet.next()) {
      System.out.println(resultSet.getString(3));
    }
    SqlQueryBuilder builder = new SqlQueryBuilder();
    builder.register(conn, AnomalyFeedback.class, "ANOMALY_FEEDBACK");

  }
}
