package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;

import java.sql.*;

public class TableController {

    @FXML private TableView<User> tableView;
    @FXML private TableColumn<User, String> nameColumn;
    @FXML private TableColumn<User, String> emailColumn;

    private final ObservableList<User> users = FXCollections.observableArrayList();

    @FXML
    public void initialize() {
        nameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
        emailColumn.setCellValueFactory(new PropertyValueFactory<>("email"));

        loadUsersFromDatabase();
        tableView.setItems(users);
    }


    private void loadUsersFromDatabase() {
        String url = "jdbc:oracle:thin:@//37.120.189.200/xe";
        String user = "EKP";
        String password = "ekp";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT Benutzer_Kennung, name FROM ela_Favoriten")) {

            while (rs.next()) {
                String name = rs.getString("Benutzer_Kennung");
                String email = rs.getString("name");
                users.add(new User(name, email));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}