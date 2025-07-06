package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.User;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.sql.*;
import java.util.Base64;
import java.util.ResourceBundle;
import javafx.fxml.Initializable;
public class TableController implements Initializable {

    private static final Logger logger = LogManager.getLogger(TableController.class);

    @FXML private ChoiceBox<String> dB_Connection;
    @FXML private ChoiceBox<String> dB_Query;

    @FXML private TextArea sqlInput;
    @FXML private TableView<ObservableList<String>> tableView;
    @FXML private Button executeButton;


    @Override
    public void initialize(URL location, ResourceBundle resources) {
        loadDbConnections();
        loadDbQueries();
    }


    public class DbConnectionInfo {
        public final String url;
        public final String user;
        public final String password;

        public DbConnectionInfo(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }
    }

    private void loadAndDisplaySqlByName(String name) {

        DbConnectionInfo connInfo = getDbConnectionFromSelection();
        if (connInfo == null) return;

        String sql = "SELECT sql FROM sql_definition WHERE name = ?";

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String query = rs.getString("sql");
                sqlInput.setText(query);  // Anzeige im Textfeld
            //    executeSql(query, connInfo);
            } else {
                showError("Nicht gefunden", "Kein SQL für '" + name + "' gefunden.");
                sqlInput.clear();
            }

        } catch (SQLException e) {
            logger.error("Fehler beim Laden von SQL für: " + name, e);
            showError("Datenbankfehler", e.getMessage());
        }
    }

    private void executeSql(String sql, DbConnectionInfo connInfo) {
        if (sql == null || sql.trim().isEmpty() ) return;

        Scene scene = tableView.getScene(); // oder eine andere bekannte Node
        scene.setCursor(Cursor.WAIT);

        executeButton.setDisable(true);

        byte[] decodedBytes = Base64.getUrlDecoder().decode(connInfo.password);

        logger.info("decodedBytes = " + new String(decodedBytes));

        //Task<ObservableList<ObservableList<String>>> task = new Task<>() {
        Task<ObservableList<ObservableList<String>>> task = new Task<ObservableList<ObservableList<String>>>() {
            @Override
            protected ObservableList<ObservableList<String>> call() throws Exception {
                try (Connection conn = DriverManager.getConnection(connInfo.url, connInfo.user, new String(decodedBytes));
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {

                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    ObservableList<TableColumn<ObservableList<String>, String>> columns = FXCollections.observableArrayList();
                    for (int i = 1; i <= columnCount; i++) {
                        final int colIndex = i - 1;
                        TableColumn<ObservableList<String>, String> column = new TableColumn<>(meta.getColumnName(i));
                        column.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                                data.getValue().get(colIndex))
                        );
                        columns.add(column);
                    }

                    ObservableList<ObservableList<String>> data = FXCollections.observableArrayList();
                    while (rs.next()) {
                        ObservableList<String> row = FXCollections.observableArrayList();
                        for (int i = 1; i <= columnCount; i++) {
                            row.add(rs.getString(i));
                        }
                        data.add(row);
                    }

                    Platform.runLater(() -> {
                        tableView.getItems().clear();
                        tableView.getColumns().setAll(columns);
                    });

                    return data;
                }
            }

            @Override
            protected void succeeded() {
                tableView.setItems(getValue());

                executeButton.setDisable(false);

                scene.setCursor(Cursor.DEFAULT);
            }

            @Override
            protected void failed() {

                scene.setCursor(Cursor.DEFAULT);
                executeButton.setDisable(false);
                Throwable e = getException();
                logger.error("SQL-Fehler", e);
                showError("Fehler", e.getMessage());
            }
        };

        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
    }




    /*
    private void executeSql(String sql, DbConnectionInfo connInfo) {
        if (sql == null || sql.isBlank()) return;

        progressIndicator.setVisible(true);
        statusLabel.setText("Abfrage wird ausgeführt...");

        byte[] decodedBytes = Base64.getUrlDecoder().decode(connInfo.password);

        logger.info("decodedBytes = " + new String(decodedBytes));

        try (Connection conn = DriverManager.getConnection(connInfo.url, connInfo.user, new String(decodedBytes));
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
            logger.error("Fehler bei der SQL-Ausführung", e.getMessage());
            logger.error("Auszuführendes SQL: " + sql );
            showError("SQL Fehler", e.getMessage());
        }
    }

     */

    private void executeSql(String sql) {
        if (sql == null || sql.trim().isEmpty()) return;

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
        String sql = "SELECT name FROM sql_definition";

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String name = rs.getString("name");
                dB_Query.getItems().add(name);
            }

            logger.info("Tabelle sql_definition erfolgreich eingelesen.");


        } catch (SQLException e) {
            logger.error("Fehler beim Laden der Tabelle sql_definition", e.getMessage());
        }

        dB_Query.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {

                tableView.getItems().clear();
                tableView.getColumns().clear();
                loadAndDisplaySqlByName(newVal);
            }
        });
        dB_Query.getSelectionModel().selectFirst();


    }


    private DbConnectionInfo getDbConnectionFromSelection() {
        String connectionName = dB_Connection.getSelectionModel().getSelectedItem();
        if (connectionName == null || connectionName.trim().isEmpty()) {
            showError("Fehler", "Keine Datenbankverbindung ausgewählt.");
            return null;
        }

        String sql = "SELECT db_url, user_name, password FROM sql_configuration WHERE name = ?";

        try (Connection conn = DriverManager.getConnection(
                DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());  // ← zentrale Adminverbindung
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, connectionName);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return new DbConnectionInfo(
                        rs.getString("db_url"),
                        rs.getString("user_name"),
                        rs.getString("password")
                );
            } else {
                showError("Nicht gefunden", "Verbindungsdaten für '" + connectionName + "' nicht gefunden.");
            }

        } catch (SQLException e) {
            logger.error("Fehler beim Laden der Verbindungsinformationen", e);
            showError("DB-Fehler", e.getMessage());
        }

        return null;
    }

    private void loadDbConnections() {
        String sql = "SELECT name FROM sql_configuration";

        try {
            // JDBC-Treiber explizit laden (nur nötig bei Java 8)
            Class.forName("oracle.jdbc.OracleDriver");

            try (
                    Connection conn = DriverManager.getConnection(
                            DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)
            ) {

                while (rs.next()) {
                    String name = rs.getString("name");
                    dB_Connection.getItems().add(name);
                    dB_Connection.getSelectionModel().selectFirst();
                }

                logger.info("DB-Verbindungen erfolgreich geladen.");

            } catch (SQLException e) {
                logger.error("Fehler beim Laden der DB-Verbindungen", e);
            }

        } catch (ClassNotFoundException e) {
            // Wird geworfen, wenn der Oracle-Treiber nicht gefunden wird
            logger.error("Oracle JDBC-Treiber nicht gefunden!", e);
        }
    }


    @FXML
    public void onExecuteQuery() {
        String sql = sqlInput.getText();
        DbConnectionInfo connInfo = getDbConnectionFromSelection();
        if (connInfo != null) {
            executeSql(sql, connInfo);
        }
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