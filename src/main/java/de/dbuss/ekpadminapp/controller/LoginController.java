package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.control.Label;
import javafx.stage.Stage;

public class LoginController {

    @FXML private TextField usernameField;
    @FXML private PasswordField passwordField;
    @FXML private Label wrongLogIn;

    @FXML
    protected void onLogin(ActionEvent event) {
        String username = usernameField.getText();
        String password = passwordField.getText();


        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/de/dbuss/ekpadminapp/view/MainView.fxml"));
            Scene tableScene = new Scene(loader.load(), 800, 600);
            Stage stage = (Stage) usernameField.getScene().getWindow();
            stage.setScene(tableScene);
            stage.setTitle("eKP Admin App");
        } catch (Exception e) {
            e.printStackTrace();
        }


        /*


        if ("admin".equals(username) && "admin".equals(password)) {
            try {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/de/dbuss/ekpadminapp/view/TableView.fxml"));
                Scene tableScene = new Scene(loader.load());
                Stage stage = (Stage) usernameField.getScene().getWindow();
                stage.setScene(tableScene);
                stage.setTitle("Data Table");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            wrongLogIn.setText("Login failed, try again");
        }


         */
    }

}