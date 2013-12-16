package com.hp.db;

import liquibase.Liquibase;
import liquibase.database.DatabaseConnection;
import liquibase.ext.vertica.database.VerticaDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.ext.vertica.snapshot.ProjectionSnapshotGenerator;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.snapshot.jvm.UniqueConstraintSnapshotGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 10/11/13
 * Time: 21:47
 * To change this template use File | Settings | File Templates.
 */
public class LiquiManager {
    public static void main(String[] args){

        VerticaDatabase verticaDatabase = new VerticaDatabase();
        liquibase.database.DatabaseFactory.getInstance().register(verticaDatabase);
        liquibase.snapshot.SnapshotGeneratorFactory.getInstance().unregister(UniqueConstraintSnapshotGenerator.class);


        Properties myProp = new Properties();

//        myProp.put("user", "z_admin");
//        myProp.put("password", "5_admin");
        myProp.put("user", "maas_admin");
        myProp.put("password", "maas_admin_123");

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(
                    "jdbc:vertica://localhost:5433/EMS",
                    myProp);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        DatabaseConnection dc = new JdbcConnection(conn);

        Liquibase liquibase = null;
        try {
//            liquibase = new Liquibase("C:\\Users\\vesterma\\Documents\\Projects\\liquibase\\target\\classes\\db\\db.changelog.xml", new FileSystemResourceAccessor(),dc);
            liquibase = new Liquibase("C:\\Users\\vesterma\\Documents\\Projects\\liquibase\\target\\classes\\db\\db_change2.xml", new FileSystemResourceAccessor(),dc);
//            liquibase.rollback(2,"");
            liquibase.update(2,"");
//            liquibase.changeLogSync("");
//            liquibase.generateDocumentation("c:\\temp");
        } catch (LiquibaseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }




    }
}
