package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;

public class TableController {

    @FXML private TableView<User> tableView;
    @FXML private TableColumn<User, String> nameColumn;
    @FXML private TableColumn<User, String> emailColumn;

    @FXML
    public void initialize() {
        nameColumn.setCellValueFactory(new PropertyValueFactory<>("name"));
        emailColumn.setCellValueFactory(new PropertyValueFactory<>("email"));

        ObservableList<User> users = FXCollections.observableArrayList(
            new User("Max Mustermann", "max@example.com"),
            new User("Erika Musterfrau", "erika@example.com")
        );
        tableView.setItems(users);
    }
}