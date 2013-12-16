package liquibase.ext.vertica.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.vertica.database.VerticaDatabase;
import liquibase.snapshot.*;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;

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

        public List<CachedRow> getProjectionDefinition(final String schemaName, final String tableName) throws SQLException, DatabaseException {
            return projectionsResultCache.get(new ResultSetCache.SingleResultSetExtractor(database) {

                @Override
                public ResultSetCache.RowData rowKeyParameters(CachedRow row) {
                    return new ResultSetCache.RowData("", row.getString("TABLE_SCHEM"), database, row.getString("TABLE_NAME"), row.getString("PROJ_NAME"));
                }

                @Override
                public ResultSetCache.RowData wantedKeyParameters() {
                    return new ResultSetCache.RowData("", schemaName, database, tableName);
                }

                @Override
                boolean shouldBulkSelect(ResultSetCache resultSetCache) {
                    Set<String> seenProjections = resultSetCache.getInfo("seenProjections", Set.class);
                    if (seenProjections == null) {
                        seenProjections = new HashSet<String>();
                        resultSetCache.putInfo("seenProjections", seenProjections);
                    }

                    seenProjections.add(schemaName + ":" + tableName);
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

                    if (!bulk) {
                        if (tableName != null) {
                            sql += " AND ANCHOR_TABLE_NAME='" + database.escapeObjectName(tableName, Table.class) + "'";
                        }
                    }
                    Statement statement = ((JdbcConnection) database.getConnection()).createStatement();
                    return statement.executeQuery(sql);
//                return this.executeQuery(sql, database);
                }
            });
    }
    }
}
