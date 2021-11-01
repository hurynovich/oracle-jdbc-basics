/*
 * Copyright (c) 1995, 2011, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.oracle.tutorial.jdbc;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import java.math.BigInteger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class StoredProcedureJavaDBSample {

    private String dbName;
    private Connection con;
    private String dbms;
    private String schema = "APP";

    public StoredProcedureJavaDBSample(Connection connArg, String dbName,
                                       String dbmsArg) {
        super();
        this.con = connArg;
        this.dbName = dbName;
        this.dbms = dbmsArg;
    }

    public static void raisePrice(String coffeeName, double maximumPercentage, BigDecimal[] newPrice) throws SQLException {

        Connection con = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        BigDecimal oldPrice;

        String queryGetCurrentCoffeePrice =
                "select COFFEES.PRICE " +
                        "from COFFEES " +
                        "where COFFEES.COF_NAME = ?";

        pstmt = con.prepareStatement(queryGetCurrentCoffeePrice);
        pstmt.setString(1, coffeeName);
        rs = pstmt.executeQuery();

        if (rs.next()) {
            oldPrice = rs.getBigDecimal(1);
        } else {
            return;
        }

        BigDecimal maximumNewPrice = oldPrice.multiply(new BigDecimal(1 + maximumPercentage));

        // Test if newPrice[0] > maximumNewPrice

        if (newPrice[0].compareTo(maximumNewPrice) == 1) {
            newPrice[0] = maximumNewPrice;
        }

        // Test if newPrice[0] <= oldPrice

        if (newPrice[0].compareTo(oldPrice) < 1) {
            newPrice[0] = oldPrice;
            return;
        }

        String queryUpdatePrice =
                "update COFFEES " +
                        "set COFFEES.PRICE = ? " +
                        "where COFFEES.COF_NAME = ?";

        pstmt = con.prepareStatement(queryUpdatePrice);
        pstmt.setBigDecimal(1, newPrice[0]);
        pstmt.setString(2, coffeeName);
        pstmt.executeUpdate();
    }


    public static void getSupplierOfCoffee(String coffeeName, String[] supplierName) throws SQLException {
        Connection con = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String query =
                "select SUPPLIERS.SUP_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where SUPPLIERS.SUP_ID = COFFEES.SUP_ID " +
                        "and ? = COFFEES.COF_NAME";

        pstmt = con.prepareStatement(query);
        pstmt.setString(1, coffeeName);
        rs = pstmt.executeQuery();

        if (rs.next()) {
            supplierName[0] = rs.getString(1);
        } else {
            supplierName[0] = null;
        }
    }

    public static void showSuppliers(ResultSet[] rs) throws SQLException {
        Connection con = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = null;
        String query =
                "select SUPPLIERS.SUP_NAME, COFFEES.COF_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where SUPPLIERS.SUP_ID = COFFEES.SUP_ID " +
                        "order by SUP_NAME";
        stmt = con.createStatement();
        rs[0] = stmt.executeQuery(query);
    }

    @Test
    public void createProcedures() throws SQLException {

        Statement stmtCreateShowSuppliers = null;
        Statement stmtCreateGetSupplierOfCoffee = null;
        Statement stmtCreateRaisePrice = null;

        Statement stmtDropShowSuppliers = null;
        Statement stmtDropGetSupplierOfCoffee = null;
        Statement stmtDropRaisePrice = null;

        String queryDropShowSuppliers = "DROP PROCEDURE SHOW_SUPPLIERS";
        String queryDropGetSupplierOfCoffee = "DROP PROCEDURE GET_SUPPLIER_OF_COFFEE";
        String queryDropRaisePrice = "DROP PROCEDURE RAISE_PRICE";

        String queryShowSuppliers =
                "CREATE PROCEDURE SHOW_SUPPLIERS() " +
                        "PARAMETER STYLE JAVA " +
                        "LANGUAGE JAVA " +
                        "DYNAMIC RESULT SETS 1 " +
                        "EXTERNAL NAME 'com.oracle.tutorial.jdbc.StoredProcedureJavaDBSample.showSuppliers'";

        String queryGetSupplierOfCoffee =
                "CREATE PROCEDURE GET_SUPPLIER_OF_COFFEE(IN coffeeName varchar(32), OUT supplierName varchar(40)) " +
                        "PARAMETER STYLE JAVA " +
                        "LANGUAGE JAVA " +
                        "DYNAMIC RESULT SETS 0 " +
                        "EXTERNAL NAME 'com.oracle.tutorial.jdbc.StoredProcedureJavaDBSample.getSupplierOfCoffee'";

        String queryRaisePrice =
                "CREATE PROCEDURE RAISE_PRICE(IN coffeeName varchar(32), IN maximumPercentage float, INOUT newPrice numeric(10,2)) " +
                        "PARAMETER STYLE JAVA " +
                        "LANGUAGE JAVA " +
                        "DYNAMIC RESULT SETS 0 " +
                        "EXTERNAL NAME 'com.oracle.tutorial.jdbc.StoredProcedureJavaDBSample.raisePrice'";

        try {
            System.out.println("Calling DROP PROCEDURE");
            stmtDropShowSuppliers = con.createStatement();
            stmtDropShowSuppliers.execute(queryDropShowSuppliers);
            stmtDropGetSupplierOfCoffee = con.createStatement();
            stmtDropGetSupplierOfCoffee.execute(queryDropGetSupplierOfCoffee);
            stmtDropRaisePrice = con.createStatement();
            stmtDropRaisePrice.execute(queryDropRaisePrice);

        } finally {
            if (stmtDropShowSuppliers != null) {
                stmtDropShowSuppliers.close();
            }
            if (stmtDropGetSupplierOfCoffee != null) {
                stmtDropGetSupplierOfCoffee.close();
            }
            if (stmtDropRaisePrice != null) {
                stmtDropRaisePrice.close();
            }
        }
        try {
            System.out.println("Calling CREATE PROCEDURE");
            stmtCreateShowSuppliers = con.createStatement();
            stmtCreateShowSuppliers.execute(queryShowSuppliers);
            stmtCreateGetSupplierOfCoffee = con.createStatement();
            stmtCreateGetSupplierOfCoffee.execute(queryGetSupplierOfCoffee);
            stmtCreateRaisePrice = con.createStatement();
            stmtCreateRaisePrice.execute(queryRaisePrice);

        } finally {
            if (stmtCreateShowSuppliers != null) {
                stmtCreateShowSuppliers.close();
            }
            if (stmtCreateGetSupplierOfCoffee != null) {
                stmtCreateGetSupplierOfCoffee.close();
            }
            if (stmtCreateRaisePrice != null) {
                stmtCreateRaisePrice.close();
            }
        }

    }

    public void registerJarFile(String jarPathName) throws SQLException {
        CallableStatement cs2 = null;
        CallableStatement cs2a = null;
        CallableStatement cs3 = null;

        String query2 =
                "CALL sqlj.install_jar('" + jarPathName + "','" + this.schema + ".JDBCTutorial',0)";
        String query2a =
                "CALL sqlj.replace_jar('" + jarPathName + "','" + this.schema + ".JDBCTutorial')";
        String query3 =
                "CALL syscs_util.syscs_set_database_property('derby.database.classpath','" +
                        this.schema + ".JDBCTutorial')";

        try {
            System.out.println("Calling " + query2);
            cs2 = con.prepareCall(query2);
            cs2.execute();
        } finally {
            if (cs2 != null) {
                cs2.close();
            }
            try {
                System.out.println("Calling " + query2a);
                cs2a = con.prepareCall(query2a);
                cs2a.execute();
            } finally {
                if (cs2a != null) {
                    cs2a.close();
                }
            }
        }
        try {
            System.out.println("Calling " + query3);
            cs3 = con.prepareCall(query3);
            cs3.execute();
        } finally {
            if (cs3 != null) {
                cs3.close();
            }
        }


    }

    @Test
    public void runStoredProcedures() throws SQLException {
        String coffeeNameArg = "Colombian";
        double maximumPercentageArg = 0.10;
        double newPriceArg = 19.99;
        CallableStatement cs = null;

        try {

            System.out.println("\nCalling the stored procedure GET_SUPPLIER_OF_COFFEE");
            cs = this.con.prepareCall("{call GET_SUPPLIER_OF_COFFEE(?, ?)}");
            cs.setString(1, coffeeNameArg);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();

            String supplierName = cs.getString(2);

            if (supplierName != null) {
                System.out.println("\nSupplier of the coffee " + coffeeNameArg + ": " + supplierName);
            } else {
                System.out.println("\nUnable to find the coffee " + coffeeNameArg);
            }

            System.out.println("\nCalling the procedure SHOW_SUPPLIERS");
            cs = this.con.prepareCall("{call SHOW_SUPPLIERS()}");
            ResultSet rs = cs.executeQuery();

            while (rs.next()) {
                String supplier = rs.getString("SUP_NAME");
                String coffee = rs.getString("COF_NAME");
                System.out.println(supplier + ": " + coffee);
            }

            System.out.println("\nContents of COFFEES table before calling RAISE_PRICE:");
            CoffeesTable.viewTable(this.con);

            System.out.println("\nCalling the procedure RAISE_PRICE");
            cs = this.con.prepareCall("{call RAISE_PRICE(?,?,?)}");
            cs.setString(1, coffeeNameArg);
            cs.setDouble(2, maximumPercentageArg);
            cs.registerOutParameter(3, Types.DOUBLE);
            cs.setDouble(3, newPriceArg);

            cs.execute();

            System.out.println("\nValue of newPrice after calling RAISE_PRICE: " + cs.getFloat(3));

            System.out.println("\nContents of COFFEES table after calling RAISE_PRICE:");
            CoffeesTable.viewTable(this.con);

        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        JDBCTutorialUtilities myJDBCTutorialUtilities;
        Connection myConnection = null;

        try {

            System.out.println("\nRunning all stored procedures:");
            runStoredProcedures("Colombian", 0.10f, 19.99f);


        } finally {
            myConnection.close();
        }

    }
}
