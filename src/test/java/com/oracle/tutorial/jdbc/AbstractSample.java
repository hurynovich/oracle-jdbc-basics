package com.oracle.tutorial.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractSample {
    protected String dbName;
    protected Connection con;
    protected String dbms;
    protected JDBCTutorialUtilities utils;

    @BeforeEach
    void init() throws Exception{
        utils = new JDBCTutorialUtilities("mysql-sample-properties.xml");
        con = utils.getConnection();
        dbName = utils.dbName;
        dbms = utils.dbms;

//        utils.executeSql("drop-tables.sql");
//        utils.executeSql("create-tables.sql");
//        utils.executeSql("populate-tables.sql");
    }

    @AfterEach
    void destruct() throws SQLException {
        con.close();
    }
}
