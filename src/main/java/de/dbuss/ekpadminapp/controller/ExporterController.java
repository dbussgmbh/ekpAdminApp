package de.dbuss.ekpadminapp.controller;


import de.dbuss.ekpadminapp.Main;
import de.dbuss.ekpadminapp.util.DBConfigResolver;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.concurrent.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
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

    private static final Logger logger = LoggerFactory.getLogger(ExporterController.class);

    @FXML
    private TextArea Output;
    @FXML
    private ProgressBar Progress;

    @FXML
    private TextField TableName;

    @FXML
    public void initialize() {

        logger.debug("Connecting to database..." + DbConfig.getUrl());

        Progress.setVisible(false);
        TableName.setVisible(false);

        try {
            oracleConn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
            //        "jdbc:oracle:thin:@37.120.189.200:1521:xe", "EKP_MONITOR", "xxxx");

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
                    Progress.setVisible(true);
                    TableName.setText("Exportiere " + selectedTable);
                    TableName.setVisible(true);
                    logger.debug("Start Export " + selectedTable + " in File " + file.getAbsolutePath());
                    Output.appendText("Start Export " + selectedTable + " in File " + file.getAbsolutePath() + "\n");
                    Output.appendText("Tabellengröße: " + getSize(selectedTable) + "GB");
                    //new Thread(() -> exportToZip(selectedTable, file.getAbsolutePath(), Output, null)).start();
                    new Thread(() -> exportToZipAsync(selectedTable, file.getAbsolutePath(), Output, Progress)).start();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getSize(String selectedTable) {
        String sizeInfo="";

        String sql="SELECT ROUND(SUM(bytes)/1024/1024/1024, 2) AS GB FROM user_segments s inner join user_lobs l on s.segment_name=l.segment_name and table_name='" + selectedTable + "'";
        logger.debug("Ausführen SQL: " + sql);
        try (PreparedStatement ps = oracleConn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                sizeInfo = rs.getString(1);
            }
        } catch (SQLException e) {
            Platform.runLater(() -> Output.appendText("Fehler beim ermitteln der Tabellengröße: " + e.getMessage() + "\n"));
        }

        return sizeInfo;
    }

    private Integer getCountRows(String tableName) {
        Integer countRows=0;

        String sql="SELECT count(*) from " + tableName;
        logger.debug("Ausführen SQL: " + sql);
        try (PreparedStatement ps = oracleConn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                countRows = rs.getInt(1);
            }
        } catch (SQLException e) {
            Platform.runLater(() -> Output.appendText("Fehler beim ermitteln der Tabellengröße: " + e.getMessage() + "\n"));
        }

        return countRows;
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

//    public static void exportToZip(String tableName, String zipPath, TextArea logArea, ProgressBar progressBar) {
//
//     //   Platform.runLater(() -> progressBar.setProgress(ProgressBar.INDETERMINATE_PROGRESS));
//        try (
//
//                Statement stmt = oracleConn.createStatement();
//                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
//                FileOutputStream fos = new FileOutputStream(zipPath);
//                ZipOutputStream zos = new ZipOutputStream(fos);
//                ByteArrayOutputStream csvStream = new ByteArrayOutputStream();
//                BufferedWriter csvWriter = new BufferedWriter(new OutputStreamWriter(csvStream))
//        ) {
//            ResultSetMetaData meta = rs.getMetaData();
//            int columnCount = meta.getColumnCount();
//
//            // Spaltenüberschriften
//            for (int i = 1; i <= columnCount; i++) {
//                csvWriter.write(meta.getColumnName(i));
//                if (i < columnCount) csvWriter.write(",");
//            }
//            csvWriter.newLine();
//
//            int rowIndex = 0;
//            int rowFortschritt = 0;
//            while (rs.next()) {
//                rowIndex++;
//                rowFortschritt++;
//                if (rowFortschritt>=10)
//                {
//                    logArea.appendText("*");
//                    rowFortschritt=0;
//                }
//
//                for (int i = 1; i <= columnCount; i++) {
//                    int type = meta.getColumnType(i);
//                    String colName = meta.getColumnName(i);
//
//                    if (type == Types.BLOB) {
//                        Blob blob = rs.getBlob(i);
//                        if (blob != null) {
//                            byte[] data = blob.getBytes(1, (int) blob.length());
//                            String blobPath = "blobs/blob_" + rowIndex + "_" + colName + ".bin";
//
//                            zos.putNextEntry(new ZipEntry(blobPath));
//                            zos.write(data);
//                            zos.closeEntry();
//
//                            csvWriter.write(blobPath);
//                        } else {
//                            csvWriter.write("NULL");
//                        }
//                    } else {
//                        Object value = rs.getObject(i);
//                        csvWriter.write(value != null ? value.toString().replaceAll("\\n", " ") : "NULL");
//                    }
//                    if (i < columnCount) csvWriter.write(",");
//                }
//                csvWriter.newLine();
//            }
//
//            csvWriter.flush();
//            zos.putNextEntry(new ZipEntry("export.csv"));
//            zos.write(csvStream.toByteArray());
//            zos.closeEntry();
//
//            int finalRowCount = rowIndex;
//            Platform.runLater(() -> logArea.appendText("Exportierte Zeilen aus " + tableName + ": " + finalRowCount + "\n"));
//        } catch (Exception e) {
//            Platform.runLater(() -> logArea.appendText("Fehler beim Export: " + e.getMessage() + "\\n"));
//        } finally {
////            Platform.runLater(() -> progressBar.setProgress(0));
//        }
//    }

    public  void exportToZipAsync(String tableName, String zipPath, TextArea logArea, ProgressBar progressBar) {

        Integer countRows = getCountRows(tableName);

        Task<Void> task = new Task<>() {
            @Override
            protected Void call() throws Exception {
                try (
                        Statement stmt = oracleConn.createStatement(
                                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                        );
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                        FileOutputStream fos = new FileOutputStream(zipPath);
                        ZipOutputStream zos = new ZipOutputStream(fos);
                        ByteArrayOutputStream csvStream = new ByteArrayOutputStream();
                        BufferedWriter csvWriter = new BufferedWriter(new OutputStreamWriter(csvStream))
                ) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    // Header
                    for (int i = 1; i <= columnCount; i++) {
                        csvWriter.write(meta.getColumnName(i));
                        if (i < columnCount) csvWriter.write(",");
                    }
                    csvWriter.newLine();

                    int rowIndex = 0;
                    int tick = 0;
                    // ggf. indeterminate aktivieren
                    while (rs.next()) {
                        rowIndex++;
                        tick++;

                        // … deine CSV/BLOB-Logik …
                        for (int i = 1; i <= columnCount; i++) {
                            int type = meta.getColumnType(i);
                            String colName = meta.getColumnName(i);
                            if (type == java.sql.Types.BLOB) {
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
                                csvWriter.write(value != null ? value.toString().replace("\n", " ") : "NULL");
                            }
                            if (i < columnCount) csvWriter.write(",");
                        }
                        csvWriter.newLine();

                        // Fortschritt + Sternchen
                        updateProgress(rowIndex, countRows);

                    }

                    csvWriter.flush();
                    zos.putNextEntry(new ZipEntry("export.csv"));
                    zos.write(csvStream.toByteArray());
                    zos.closeEntry();

                    int finalRowCount = rowIndex;
                    Platform.runLater(() ->
                            {
                            logArea.appendText("\nExportierte Zeilen aus " + tableName + ": " + finalRowCount + "\n");
                            Progress.setVisible(false);
                                TableName.setVisible(false);
                            }
                    );
                } catch (Exception e) {
                    Platform.runLater(() -> logArea.appendText("\nFehler beim Export: " + e.getMessage() + "\n"));
                }
                return null;
            }
        };

        // ProgressBar binden (indeterminate, wenn progress = -1/-1)
        Progress.progressProperty().bind(task.progressProperty());

        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }




}
