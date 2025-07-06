package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import de.dbuss.ekpadminapp.service.DatabaseService;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.net.URL;
import java.sql.*;
import java.util.Hashtable;
import java.util.List;
import java.util.ResourceBundle;

public class MessageExportController implements Initializable {
    private static final Logger logger = LogManager.getLogger(MessageExportController.class);
    @FXML
    private Button LoginBtn;

    @FXML
    public void onLogin()
    {

        LoginBtn.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent actionEvent) {
                //System.out.println("Connect Button gedrückt");
                logger.info("Connect Button gedrückt");


                String url = "t3://LAP6.fritz.box:7001";  // ✅ Deine IP + Port
                //String url = "t3://37.120.190.179:7001";  // ✅ Deine IP + Port
                String username = "admin";             // Benutzername
                String password = "7x24!admin4me";             // Passwort

                Hashtable<String, String> env = new Hashtable<>();
                env.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
                env.put(Context.PROVIDER_URL, url);
                env.put(Context.SECURITY_PRINCIPAL, username);
                env.put(Context.SECURITY_CREDENTIALS, password);

                String jndiName = "jdbc/ekpDataSource";
                try {
                    // 1. JNDI-Context aufbauen
                    Context ctx = new InitialContext(env);

                    // 2. DataSource per JNDI holen
                    //DataSource ds = (DataSource) ctx.lookup(jndiName);

                    DataSource ds = (DataSource) ctx.lookup("jdbc/ekpDataSource");
                    System.out.println("✅ DataSource gefunden: " + jndiName);

                    // 3. Verbindung holen und Abfrage ausführen
                    try (

//                    Connection conn = ds.getConnection();
                            Connection conn = ds.getConnection(); // Dieser Aufruf "arbeitet" serverseitig

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

                    ctx.close();

                } catch (Exception e) {
                    System.out.println("❌ Fehler beim Zugriff auf die DataSource: " + e.getMessage());
                    e.printStackTrace();
                }













                //List<User> users = DatabaseService.getUsersFromDatabase();


            }
        });
    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

    }
}