package de.dbuss.ekpadminapp.service;

import de.dbuss.ekpadminapp.model.User;


import javax.naming.Context;
import javax.naming.InitialContext;
import javax.rmi.PortableRemoteObject;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class DatabaseService {
    public static List<User> getUsersFromDatabase() {

        String url = "t3://LAP6.fritz.box:7001";
        String username = "admin";             // Benutzername
        String password = "7x24!admin4me";             // Passwort
        String jndiName = "jdbc/ekpDataSource";

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
        env.put(Context.PROVIDER_URL, url);
        env.put(Context.SECURITY_PRINCIPAL, username);
        env.put(Context.SECURITY_CREDENTIALS, password);

        List<User> userList = new ArrayList<>();
        javax.sql.DataSource ds=null;
        try {
            // 1. JNDI-Context aufbauen
         //   Context ctx = new InitialContext(env);
            Context context=new InitialContext( env );

            // 2. DataSource per JNDI holen
            //DataSource ds = (DataSource) ctx.lookup(jndiName);

            //DataSource ds = (DataSource) ctx.lookup("jdbc/ekpDataSource");

            ds=(javax.sql.DataSource) context.lookup ("jdbc/ekpDataSource");

            System.out.println("✅ DataSource gefunden: " + jndiName);

            // 3. Verbindung holen und Abfrage ausführen
            try (

//                    Connection conn = ds.getConnection();
                    Connection conn =ds.getConnection(); // Dieser Aufruf "arbeitet" serverseitig

                    PreparedStatement stmt = conn.prepareStatement("select * from ekp.users");
                    ResultSet rs = stmt.executeQuery()) {

                System.out.println("📄 Ergebnisse:");

                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                while (rs.next()) {
                    for (int i = 1; i <= colCount; i++) {
                        System.out.print(rs.getString(i));
                        if (i < colCount) System.out.print(" | ");
                    }
                    System.out.println();
                }

            } catch (SQLException e) {
                System.out.println("❌ SQL-Fehler: " + e.getMessage());
                e.printStackTrace();
            }

            //ctx.close();
            context.close();

        } catch (Exception e) {
            System.out.println("❌ Fehler beim Zugriff auf die DataSource: " + e.getMessage());
            e.printStackTrace();
        }

        return userList;
    }
}
