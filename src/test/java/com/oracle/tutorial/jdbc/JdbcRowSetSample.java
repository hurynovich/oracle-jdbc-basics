/*
 * Copyright (c) 1995, 2020, Oracle and/or its affiliates. All rights reserved.
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

import javax.sql.rowset.JdbcRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.RowSet;

public class JdbcRowSetSample extends AbstractSample {

    @Test
    public void testJdbcRowSet() throws SQLException {

        RowSetFactory factory = RowSetProvider.newFactory();

        try (JdbcRowSet jdbcRs = factory.createJdbcRowSet()) {
            jdbcRs.setUrl(this.utils.urlString);
            jdbcRs.setUsername(this.utils.userName);
            jdbcRs.setPassword(this.utils.password);
            jdbcRs.setCommand("select * from COFFEES");
            jdbcRs.execute();
            jdbcRs.absolute(3);
            jdbcRs.updateFloat("PRICE", 10.99f);
            jdbcRs.updateRow();

            System.out.println("\nAfter updating the third row:");
            CoffeesTable.viewTable(con);

            jdbcRs.moveToInsertRow();
            jdbcRs.updateString("COF_NAME", "HouseBlend");
            jdbcRs.updateInt("SUP_ID", 49);
            jdbcRs.updateFloat("PRICE", 7.99f);
            jdbcRs.updateInt("SALES", 0);
            jdbcRs.updateInt("TOTAL", 0);
            jdbcRs.insertRow();

            jdbcRs.moveToInsertRow();
            jdbcRs.updateString("COF_NAME", "HouseDecaf");
            jdbcRs.updateInt("SUP_ID", 49);
            jdbcRs.updateFloat("PRICE", 8.99f);
            jdbcRs.updateInt("SALES", 0);
            jdbcRs.updateInt("TOTAL", 0);
            jdbcRs.insertRow();

            System.out.println("\nAfter inserting two rows:");
            CoffeesTable.viewTable(con);

            jdbcRs.last();
            jdbcRs.deleteRow();

            System.out.println("\nAfter deleting last row:");
            CoffeesTable.viewTable(con);


        } finally {
            this.con.setAutoCommit(false);
        }
    }

    private void outputRowSet(RowSet rs) throws SQLException {
        rs.beforeFirst();
        while (rs.next()) {
            String coffeeName = rs.getString(1);
            int supplierID = rs.getInt(2);
            float price = rs.getFloat(3);
            int sales = rs.getInt(4);
            int total = rs.getInt(5);
            System.out.println(coffeeName + ", " + supplierID + ", " + price +
                    ", " + sales + ", " + total);

        }
    }
}
