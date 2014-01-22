package liquibase.ext.vertica.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.vertica.database.VerticaDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.structure.DatabaseObject;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 27/11/13
 * Time: 14:11
 * To change this template use File | Settings | File Templates.
 */
public class VerticaDatabaseSnapshot extends JdbcDatabaseSnapshot {
    VerticaCachingDatabaseMetaData verticaCachingDatabaseMetaData;
    ResultSetCache projectionsResultCache;
    public VerticaDatabaseSnapshot(DatabaseObject[] examples, Database database, SnapshotControl snapshotControl) throws DatabaseException, InvalidExampleException {
        super(examples, database, snapshotControl);
        projectionsResultCache = new ResultSetCache();

    }

    public VerticaDatabaseSnapshot(DatabaseObject[] examples, Database database) throws DatabaseException, InvalidExampleException {
        super(examples, database);
        projectionsResultCache = new ResultSetCache();
    }


    public VerticaCachingDatabaseMetaData getMetaData() throws SQLException {
        if (verticaCachingDatabaseMetaData == null) {
            DatabaseMetaData databaseMetaData = null;
            if (getDatabase().getConnection() != null) {
                databaseMetaData = ((JdbcConnection) getDatabase().getConnection()).getUnderlyingConnection().getMetaData();
            }
            verticaCachingDatabaseMetaData = new VerticaCachingDatabaseMetaData(this.getDatabase(), databaseMetaData);
        }
        return verticaCachingDatabaseMetaData;
    }


    public class VerticaCachingDatabaseMetaData extends JdbcDatabaseSnapshot.CachingDatabaseMetaData {
        private DatabaseMetaData databaseMetaData;
        private Database database;

        public VerticaCachingDatabaseMetaData(Database database, DatabaseMetaData metaData) {
            super(database, metaData);
            this.databaseMetaData = metaData;
            this.database = database;
        }

        public List<CachedRow> getProjectionDefinition(final String schemaName, final String projection) throws SQLException, DatabaseException { //, final String tableName)
            return projectionsResultCache.get(new ResultSetCache.SingleResultSetExtractor(database) {

                @Override
                public ResultSetCache.RowData rowKeyParameters(CachedRow row) {
                    return new ResultSetCache.RowData(null, row.getString("TABLE_SCHEM"), database, row.getString("PROJ_NAME"));
//                    return new ResultSetCache.RowData(null, row.getString("TABLE_SCHEM"), database, row.getString("TABLE_NAME"), row.getString("PROJ_NAME"));
                }

                @Override
                public ResultSetCache.RowData wantedKeyParameters() {
//                    return new ResultSetCache.RowData(null, schemaName, database, tableName,null);
                    return new ResultSetCache.RowData(null, schemaName, database, projection);
                }

                @Override
                boolean shouldBulkSelect(ResultSetCache resultSetCache) {
                    Set<String> seenProjections = resultSetCache.getInfo("seenProjections", Set.class);
                    if (seenProjections == null) {
                        seenProjections = new HashSet<String>();
                        resultSetCache.putInfo("seenProjections", seenProjections);
                    }

//                    seenProjections.add(schemaName + ":" + tableName);
                    seenProjections.add(schemaName);
                    return seenProjections.size() > 2;
                }

                @Override
                public ResultSet fastFetchQuery() throws SQLException, DatabaseException {
                    if (database instanceof VerticaDatabase) {
                        return verticaQuery(false);
                    }
                    return null;
                }

                @Override
                public ResultSet bulkFetchQuery() throws SQLException, DatabaseException {
                    if (database instanceof VerticaDatabase) {
                        return verticaQuery(true);
                    }
                    return null;
                }

                protected ResultSet verticaQuery(boolean bulk) throws DatabaseException, SQLException {
                    CatalogAndSchema catalogAndSchema = database.correctSchema(new CatalogAndSchema("", schemaName));

                    String sql = "select PROJECTION_SCHEMA AS TABLE_SCHEM, PROJECTION_NAME AS PROJ_NAME, ANCHOR_TABLE_NAME AS TABLE_NAME " +
                            "FROM V_CATALOG.PROJECTIONS " +
                            "WHERE PROJECTION_SCHEMA ='" + ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema) + "'";

//                    if (!bulk) {
//                        if (tableName != null) {
//                            sql += " AND ANCHOR_TABLE_NAME='" + database.escapeObjectName(tableName, Table.class) + "'";
//                        }
//                    }
                    Statement statement = ((JdbcConnection) database.getConnection()).createStatement();
                    return statement.executeQuery(sql);
//                return this.executeQuery(sql, database);
                }
            });
    }

        public List<CachedRow> getProjectionColumns(final String schemaName, final String projection, final String columnName) throws SQLException, DatabaseException { //, final String tableName)
            return projectionsResultCache.get(new ResultSetCache.SingleResultSetExtractor(database) {

                @Override
                public ResultSetCache.RowData rowKeyParameters(CachedRow row) {
                    return new ResultSetCache.RowData(null, row.getString("TABLE_SCHEM"), database, row.getString("PROJ_NAME"), row.getString("COLUMN_NAME"));
//                    return new ResultSetCache.RowData(null, row.getString("TABLE_SCHEM"), database, row.getString("TABLE_NAME"), row.getString("PROJ_NAME"));
                }

                @Override
                public ResultSetCache.RowData wantedKeyParameters() {
                    return new ResultSetCache.RowData(null, schemaName, database, projection, columnName);
                }

                @Override
                boolean shouldBulkSelect(ResultSetCache resultSetCache) {
                    Set<String> seenProjections = resultSetCache.getInfo("seenProjections", Set.class);
                    if (seenProjections == null) {
                        seenProjections = new HashSet<String>();
                        resultSetCache.putInfo("seenProjections", seenProjections);
                    }

                    seenProjections.add(schemaName + ":" + projection);
//                    seenProjections.add(schemaName);
                    return seenProjections.size() > 2;
                }

                @Override
                public ResultSet fastFetchQuery() throws SQLException, DatabaseException {
                    if (database instanceof VerticaDatabase) {
                        return verticaQuery(false);
                    }
                    return null;
                }

                @Override
                public ResultSet bulkFetchQuery() throws SQLException, DatabaseException {
                    if (database instanceof VerticaDatabase) {
                        return verticaQuery(true);
                    }
                    return null;
                }

                protected ResultSet verticaQuery(boolean bulk) throws DatabaseException, SQLException {
                    CatalogAndSchema catalogAndSchema = database.correctSchema(new CatalogAndSchema("", schemaName));

                    String sql = "select p.projection_schema AS TABLE_SCHEM,pc.projection_name AS PROJ_NAME,pc.projection_column_name AS COLUMN_NAME, " +
                            "c.data_type AS TYPE_NAME, c.DATA_TYPE_ID AS DATA_TYPE,pc.encoding_type, c.is_nullable AS NULLABLE, IS_IDENTITY AS IS_AUTOINCREMENT," +
                            "pc.ENCODING_TYPE AS ENCODING " +
                            "from projection_columns pc " +
                            "join projections p on (p.projection_id = pc.projection_id) " +
                            "join columns     c on (pc.table_column_id = c.column_id) " +
                            "WHERE p.PROJECTION_SCHEMA ='" + ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema) + "'";

//                    if (!bulk) {
//                        if (tableName != null) {
//                            sql += " AND ANCHOR_TABLE_NAME='" + database.escapeObjectName(tableName, Table.class) + "'";
//                        }
//                    }
                    Statement statement = ((JdbcConnection) database.getConnection()).createStatement();
                    return statement.executeQuery(sql);
//                return this.executeQuery(sql, database);
                }
            });
        }
    }
}

