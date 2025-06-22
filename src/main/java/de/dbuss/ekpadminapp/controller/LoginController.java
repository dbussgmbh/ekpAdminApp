package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.control.Label;
import javafx.stage.Stage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import java.sql.*;

public class LoginController {

    private static final Logger logger = LogManager.getLogger(LoginController.class);

    @FXML private TextField usernameField;
    @FXML private PasswordField passwordField;
    @FXML private Label wrongLogIn;

    @FXML
    protected void onLogin(ActionEvent event) {
        String username = usernameField.getText();
        String password = passwordField.getText();

      //  if (authenticateUser(username, password)) {
        if (true) {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/de/dbuss/ekpadminapp/view/MainView.fxml"));
                Scene tableScene = new Scene(loader.load());
                Stage stage = (Stage) usernameField.getScene().getWindow();
                stage.setScene(tableScene);
                stage.setTitle("eKP Admin App");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            wrongLogIn.setText("Login failed, try again");
        }


    }

    private boolean authenticateUser(String username, String password) {
        String sql = "SELECT hashed_password FROM FVMADM_USER WHERE username = ?";
        logger.debug("Anmeldeversuch User: " + username);

        logger.trace("DB-URL: " + DbConfig.getUrl());
        logger.trace("DB-User: " + DbConfig.getUser());
        logger.trace("DB-Password: " + DbConfig.getPassword());

        try (Connection conn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, username);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                logger.debug("User " + username + " wurde in Tabelle ekp_monitor.FVM_USER gefunden.");
                String hashedPassword = rs.getString("hashed_password");

                if (BCrypt.checkpw(password, hashedPassword)) {
                    logger.debug("Passwort stimmt überein");
                    return true;
                }
                else {
                    logger.debug("Passwort stimmt nicht überein!");
                    return false;
                }
            }
            else {
                logger.error("User " + username + " wurde nicht in Tabelle ekp_monitor.FVM_USER gefunden!");
            }

        } catch (SQLException e) {
            logger.error(e.getMessage());
            //e.printStackTrace();
        }
        return false;
    }

}