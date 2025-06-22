package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.sql.*;
import java.util.ResourceBundle;
import javafx.fxml.Initializable;
public class TableController implements Initializable {

    private static final Logger logger = LogManager.getLogger(TableController.class);

    @FXML private ChoiceBox<String> dB_Connection;
    @FXML private ChoiceBox<String> dB_Query;

    @FXML private TextField sqlInput;
    @FXML private TableView<ObservableList<String>> tableView;

    private final ObservableList<User> users = FXCollections.observableArrayList();

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        loadDbConnections();
        loadDbQueries();
    }


    private void loadAndDisplaySqlByName(String name) {
        String sql = "SELECT sql FROM sql_definition WHERE name = ?";

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String query = rs.getString("sql");
                sqlInput.setText(query);  // 👉 Anzeige im Textfeld
            } else {
                showError("Nicht gefunden", "Kein SQL für '" + name + "' gefunden.");
                sqlInput.clear();
            }

        } catch (SQLException e) {
            logger.error("Fehler beim Laden von SQL für: " + name, e);
            showError("Datenbankfehler", e.getMessage());
        }
    }

    private void executeSql(String sql) {
        if (sql == null || sql.isBlank()) return;

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            tableView.getItems().clear();
            tableView.getColumns().clear();

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                final int colIndex = i - 1;
                TableColumn<ObservableList<String>, String> column = new TableColumn<>(meta.getColumnName(i));
                column.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                        data.getValue().get(colIndex))
                );
                tableView.getColumns().add(column);
            }

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
            logger.error("Fehler bei der SQL-Ausführung", e);
            showError("SQL Fehler", e.getMessage());
        }
    }

    private void loadDbQueries() {
        String sql = "SELECT name,sql FROM sql_definition";

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String name = rs.getString("name");
           //     String sql = rs.getString("sql");
                dB_Query.getItems().add(name);
            }

            logger.info("SQL-Definitionen erfolgreich geladen.");



        } catch (SQLException e) {
            logger.error("Fehler beim Laden der SQL-Definitionen", e);
        }

        dB_Query.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                loadAndDisplaySqlByName(newVal);
            }
        });


    }




    private void loadDbConnections() {
        String sql = "SELECT name FROM sql_configuration";

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String name = rs.getString("name");
                dB_Connection.getItems().add(name);
            }

            logger.info("DB-Verbindungen erfolgreich geladen.");

        } catch (SQLException e) {
            logger.error("Fehler beim Laden der DB-Verbindungen", e);
        }
    }


    @FXML
    public void onExecuteQuery() {
        executeSql(sqlInput.getText());
    }

    /*

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

     */

    private void showError(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

}