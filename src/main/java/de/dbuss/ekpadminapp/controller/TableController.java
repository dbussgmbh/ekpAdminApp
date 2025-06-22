package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

public class TableController {

    private static final Logger logger = LogManager.getLogger(TableController.class);

    @FXML private TableColumn<User, String> nameColumn;
    @FXML private TableColumn<User, String> emailColumn;

    @FXML private TextField sqlInput;
    @FXML private TableView<ObservableList<String>> tableView;

    private final ObservableList<User> users = FXCollections.observableArrayList();

    String url = "jdbc:oracle:thin:@//37.120.189.200/xe";
    String user = "EKP";
    String password = "ekp";



    @FXML
    public void onExecuteQuery() {
        String sql = sqlInput.getText();
        if (sql == null || sql.isBlank()) return;


        logger.trace("DB-URL: " + DbConfig.getUrl());
        logger.trace("DB-User: " + DbConfig.getUser());
        logger.trace("DB-Password: " + DbConfig.getPassword());

        try (Connection conn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            tableView.getItems().clear();
            tableView.getColumns().clear();

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            // Spalten dynamisch erstellen
            for (int i = 1; i <= columnCount; i++) {
                final int colIndex = i - 1;
                TableColumn<ObservableList<String>, String> column = new TableColumn<>(meta.getColumnName(i));
                column.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                        data.getValue().get(colIndex))
                );
                tableView.getColumns().add(column);
            }

            // Zeilen füllen
            ObservableList<ObservableList<String>> data = FXCollections.observableArrayList();
            while (rs.next()) {
                ObservableList<String> row = FXCollections.observableArrayList();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                data.add(row);
            }

            tableView.setItems(data);

        } catch (SQLException e) {
            showError("SQL Fehler", e.getMessage());
            e.printStackTrace();
        }
    }

    private void showError(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

}