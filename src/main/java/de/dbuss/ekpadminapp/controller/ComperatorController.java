package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.model.QueryModel;
import de.dbuss.ekpadminapp.util.DBConfigResolver;
import de.dbuss.ekpadminapp.util.DBQueryExecutor;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.beans.property.SimpleStringProperty;
import javafx.stage.FileChooser;
import javafx.collections.*;
import javafx.collections.transformation.FilteredList;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class ComperatorController {
    @FXML
    private TableView<Map<String, String>> tableView;

    @FXML
    private Button loadButton;
    @FXML
    private TextField filterField;

    private DBConfigResolver resolver;
    private Map<String, String> dbMap;

    private ObservableList<Map<String, String>> data = FXCollections.observableArrayList();
    private FilteredList<Map<String, String>> filteredData;

    @FXML
    public void initialize() {
        try {
            Connection oracleConn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
            //        "jdbc:oracle:thin:@37.120.189.200:1521:xe", "EKP_MONITOR", "xxxx");

            resolver = new DBConfigResolver(oracleConn);
            dbMap = resolver.resolveConnections();

            setupFilter();
            //refresh();
            //initTable();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setupFilter() {
        filteredData = new FilteredList<>(data, p -> true);
        filterField.textProperty().addListener((obs, oldVal, newVal) -> {
            filteredData.setPredicate(row -> {
                if (newVal == null || newVal.isEmpty()) return true;
                String lower = newVal.toLowerCase();
                return row.values().stream().anyMatch(v -> v != null && v.toLowerCase().contains(lower));
            });
        });
        tableView.setItems(filteredData);
    }

    @FXML
    private void onRefreshClick() {
        refresh();

    }



    private void refresh() {

        Scene scene = tableView.getScene();
        if (scene == null) return;

        scene.setCursor(Cursor.WAIT);
        loadButton.setText("⏳ Lädt...");
        loadButton.setDisable(true);

        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                List<Map<String, String>> result = loadData(); // führt DB-Zugriffe durch
                Platform.runLater(() -> updateTable(result));   // GUI aktualisieren
                return null;
            }
        };

        task.setOnSucceeded(e -> {
            scene.setCursor(Cursor.DEFAULT);
            loadButton.setText("Refresh");
            loadButton.setDisable(false);
        });
        task.setOnFailed(e -> {
            scene.setCursor(Cursor.DEFAULT);
            task.getException().printStackTrace();
        });

        new Thread(task).start();


    }



    private void updateTable(List<Map<String, String>> newData) {
        data.clear();
        tableView.getColumns().clear();

        data.addAll(newData);

        Set<String> allKeys = new LinkedHashSet<>();
        for (Map<String, String> row : data) {
            allKeys.addAll(row.keySet());
        }

        for (String key : allKeys) {
            TableColumn<Map<String, String>, String> col = new TableColumn<>(key);
            col.setCellValueFactory(data -> new SimpleStringProperty(
                    data.getValue().getOrDefault(key, "")
            ));

            if (!key.equals("SQL")) {
                col.setCellFactory(column -> new TableCell<Map<String, String>, String>() {
                    @Override
                    protected void updateItem(String item, boolean empty) {
                        super.updateItem(item, empty);
                        setText(item);
                        setStyle("");
                        if (!empty && item != null) {
                            Map<String, String> row = getTableView().getItems().get(getIndex());
                            String referenceValue = row.entrySet().stream()
                                    .filter(e -> !e.getKey().equals("SQL"))
                                    .map(Map.Entry::getValue)
                                    .findFirst().orElse(null);

                            if (referenceValue != null && !referenceValue.equals(item)) {
                                setStyle("-fx-background-color: lightcoral; -fx-text-fill: black;");
                            }
                        }
                    }
                });
            }

            tableView.getColumns().add(col);
        }

        if (filteredData != null)
            filteredData.setPredicate(null);
        tableView.setItems(data);
    }

    private List<Map<String, String>> loadData() throws Exception {
        List<Map<String, String>> rows = new ArrayList<>();
        List<QueryModel> queries = resolver.loadQueries();

        for (QueryModel qm : queries) {
            Map<String, String> row = new HashMap<>();
            row.put("SQL", qm.getSql());
            for (String db : qm.getDbKuerzel()) {
                if (dbMap.containsKey(db)) {
                    String[] parts = dbMap.get(db).split(";");
                    String jdbcUrl = parts[0];
                    String user = parts[1];
                    String pass = parts[2];
                    System.out.printf("==> Kürzel: %s → URL: %s | USER: %s | PASS: %s%n", db, jdbcUrl, user, pass);

                    String result = DBQueryExecutor.execute(jdbcUrl, user, pass, qm.getSql());
                    row.put(db, result);
                } else {
                    row.put(db, "Unbekannt");
                }
            }
            rows.add(row);
        }

        return rows;
    }


    @FXML
    private void onExportClick() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("CSV speichern");
        chooser.setInitialFileName("vergleich.csv");
        File file = chooser.showSaveDialog(tableView.getScene().getWindow());
        if (file != null) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
                List<String> headers = new ArrayList<>(tableView.getColumns().size());
                for (TableColumn<Map<String, String>, ?> col : tableView.getColumns()) {
                    headers.add(col.getText());
                }
                writer.println(String.join(";", headers));

                for (Map<String, String> row : filteredData) {
                    List<String> values = new ArrayList<>();
                    for (String header : headers) {
                        values.add(row.getOrDefault(header, ""));
                    }
                    writer.println(String.join(";", values));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void initTable() {
        data.clear();
        tableView.getColumns().clear();

        Scene scene = tableView.getScene();

        System.out.println("tableView: " + tableView.toString());

        System.out.println("Scene: " + scene);

      //  if (scene == null) return;

      //  scene.setCursor(Cursor.WAIT);

        try {
            List<QueryModel> queries = resolver.loadQueries();

            for (QueryModel qm : queries) {
                Map<String, String> row = new HashMap<>();
                row.put("SQL", qm.getSql());
                for (String db : qm.getDbKuerzel()) {
                    if (dbMap.containsKey(db)) {
                        String[] parts = dbMap.get(db).split(";");
                        String jdbcUrl = parts[0];
                        String user = parts[1];
                        String pass = parts[2];

                        System.out.printf("==> Kürzel: %s → URL: %s | USER: %s | PASS: %s%n", db, jdbcUrl, user, pass);

                        String result = DBQueryExecutor.execute(jdbcUrl, user, pass, qm.getSql());
                        row.put(db, result);
                    } else {
                        row.put(db, "Unbekannt");
                    }
                }
                data.add(row);
            }

            Set<String> allKeys = new LinkedHashSet<>();
            for (Map<String, String> row : data) {
                allKeys.addAll(row.keySet());
            }

            for (String key : allKeys) {
                TableColumn<Map<String, String>, String> col = new TableColumn<>(key);
                col.setCellValueFactory(data -> new SimpleStringProperty(
                        data.getValue().getOrDefault(key, "")
                ));
                col.setSortable(true);

                if (!key.equals("SQL")) {
                    col.setCellFactory(column -> new TableCell<Map<String, String>, String>() {
                        @Override
                        protected void updateItem(String item, boolean empty) {
                            super.updateItem(item, empty);
                            setText(item);
                            setStyle("");

                            if (!empty && item != null) {
                                Map<String, String> row = getTableView().getItems().get(getIndex());
                                String referenceValue = null;

                                for (Map.Entry<String, String> entry : row.entrySet()) {
                                    if (!entry.getKey().equals("SQL")) {
                                        referenceValue = entry.getValue();
                                        break;
                                    }
                                }

                                if (referenceValue != null && !referenceValue.equals(item)) {
                                    setStyle("-fx-background-color: lightcoral; -fx-text-fill: black;");
                                }
                            }
                        }
                    });
                }

                tableView.getColumns().add(col);
            }

       //     scene.setCursor(Cursor.DEFAULT);

        } catch (Exception ex) {
            //scene.setCursor(Cursor.DEFAULT);
            ex.printStackTrace();
        }
    }
}
