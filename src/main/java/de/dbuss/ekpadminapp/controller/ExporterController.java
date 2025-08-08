package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.Main;
import de.dbuss.ekpadminapp.model.QueryModel;
import de.dbuss.ekpadminapp.util.DBConfigResolver;
import de.dbuss.ekpadminapp.util.DBQueryExecutor;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.Cursor;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ExporterController {

    private DBConfigResolver resolver;
    private Map<String, String> dbMap;

    private ObservableList<Map<String, String>> data = FXCollections.observableArrayList();
    private FilteredList<Map<String, String>> filteredData;

    private static Connection oracleConn;

    @FXML
    private TextArea Output;

    @FXML
    public void initialize() {
        try {
            oracleConn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
            //        "jdbc:oracle:thin:@37.120.189.200:1521:xe", "EKP_MONITOR", "xxxx");

            resolver = new DBConfigResolver(oracleConn);
            dbMap = resolver.resolveConnections();


            //refresh();
            //initTable();

            List<String> tables = fetchTableNames();
            if (tables.isEmpty()) {
                System.out.println("Keine Tabellen gefunden - Die Datenbank enthält keine benutzerdefinierten Tabellen.");
                return;
            }

            Stage stage = Main.getPrimaryStage();

            ChoiceDialog<String> dialog = new ChoiceDialog<>(tables.get(0), tables);
            dialog.setTitle("Tabelle wählen");
            dialog.setHeaderText("Welche Tabelle möchtest du exportieren?");
            dialog.setContentText("Tabelle:");

            dialog.showAndWait().ifPresent(selectedTable -> {
                FileChooser fc = new FileChooser();
                fc.setTitle("ZIP-Datei für Export wählen");
                fc.setInitialFileName("export_" + selectedTable + ".zip");
                fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("ZIP-Dateien", "*.zip"));
                File file = fc.showSaveDialog(stage);
                if (file != null) {
                    new Thread(() -> exportToZip(selectedTable, file.getAbsolutePath(), Output, null)).start();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> fetchTableNames() {
        List<String> tables = new ArrayList<>();

        try (PreparedStatement ps = oracleConn.prepareStatement("SELECT table_name FROM user_tables ORDER BY table_name");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            Platform.runLater(() -> Output.appendText("Fehler beim Laden der Tabellen: " + e.getMessage() + "\\n"));
        }
        return tables;
    }

    public static void exportToZip(String tableName, String zipPath, TextArea logArea, ProgressBar progressBar) {

        Platform.runLater(() -> progressBar.setProgress(ProgressBar.INDETERMINATE_PROGRESS));
        try (

                Statement stmt = oracleConn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                FileOutputStream fos = new FileOutputStream(zipPath);
                ZipOutputStream zos = new ZipOutputStream(fos);
                ByteArrayOutputStream csvStream = new ByteArrayOutputStream();
                BufferedWriter csvWriter = new BufferedWriter(new OutputStreamWriter(csvStream))
        ) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            // Spaltenüberschriften
            for (int i = 1; i <= columnCount; i++) {
                csvWriter.write(meta.getColumnName(i));
                if (i < columnCount) csvWriter.write(",");
            }
            csvWriter.newLine();

            int rowIndex = 0;
            while (rs.next()) {
                rowIndex++;
                for (int i = 1; i <= columnCount; i++) {
                    int type = meta.getColumnType(i);
                    String colName = meta.getColumnName(i);

                    if (type == Types.BLOB) {
                        Blob blob = rs.getBlob(i);
                        if (blob != null) {
                            byte[] data = blob.getBytes(1, (int) blob.length());
                            String blobPath = "blobs/blob_" + rowIndex + "_" + colName + ".bin";

                            zos.putNextEntry(new ZipEntry(blobPath));
                            zos.write(data);
                            zos.closeEntry();

                            csvWriter.write(blobPath);
                        } else {
                            csvWriter.write("NULL");
                        }
                    } else {
                        Object value = rs.getObject(i);
                        csvWriter.write(value != null ? value.toString().replaceAll("\\n", " ") : "NULL");
                    }
                    if (i < columnCount) csvWriter.write(",");
                }
                csvWriter.newLine();
            }

            csvWriter.flush();
            zos.putNextEntry(new ZipEntry("export.csv"));
            zos.write(csvStream.toByteArray());
            zos.closeEntry();

            int finalRowCount = rowIndex;
            Platform.runLater(() -> logArea.appendText("Exportierte Zeilen aus " + tableName + ": " + finalRowCount + "\\n"));
        } catch (Exception e) {
            Platform.runLater(() -> logArea.appendText("Fehler beim Export: " + e.getMessage() + "\\n"));
        } finally {
            Platform.runLater(() -> progressBar.setProgress(0));
        }
    }

}
