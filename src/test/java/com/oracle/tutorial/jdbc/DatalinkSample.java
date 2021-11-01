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

// Java io imports

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

// Java net imports
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;

// SQL imports
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatalinkSample extends AbstractSample {

    @Test
    public void viewTable() throws SQLException, IOException {
        String query = "SELECT document_name, url FROM data_repository";
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                String documentName = null;
                java.net.URL url = null;
                documentName = rs.getString(1);
                // Retrieve the value as a URL object.
                url = rs.getURL(2);
                if (url != null) {
                    // Retrieve the contents from the URL.
                    URLConnection myURLConnection = url.openConnection(Proxy.NO_PROXY);
                    BufferedReader bReader =
                            new BufferedReader(new InputStreamReader(myURLConnection.getInputStream()));
                    System.out.println("Document name: " + documentName);
                    String pageContent = null;
                    while ((pageContent = bReader.readLine()) != null) {
                        // Print the URL contents
                        System.out.println(pageContent);
                    }
                } else {
                    System.out.println("URL is null");
                }
            }
        } catch (IOException ioEx) {
            System.out.println("IOException caught: " + ioEx.toString());
        } catch (Exception ex) {
            System.out.println("Unexpected exception");
            ex.printStackTrace();
        }
    }


    public void addURLRow(String description, String url) throws SQLException {
        String query = "INSERT INTO data_repository(document_name,url) VALUES (?,?)";
        try (PreparedStatement pstmt = this.con.prepareStatement(query)) {
            pstmt.setString(1, description);
            pstmt.setURL(2, new URL(url));
            pstmt.execute();
        } catch (Exception ex) {
            System.out.println("Unexpected exception");
            ex.printStackTrace();
        }
    }

}