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
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.DataFormat;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.TransferMode;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;
import javafx.concurrent.Task;
import org.apache.logging.log4j.core.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private TextField directoryField;

    @FXML
    private ProgressBar Progress;

    @FXML
    private Label InfoLabel;


    @FXML private ListView<String> leftList;
    @FXML private ListView<String> rightList;
    @FXML private Button toRight;
    @FXML private Button allToRight;
    @FXML private Button toLeft;
    @FXML private Button allToLeft;


    // --- Button-Handler (von FXML gebunden) ---
    @FXML private void onToRight()    { moveSelected(leftList, leftItems, rightItems);  sortIfNeeded(rightItems); }
    @FXML private void onAllToRight() { moveAll(leftItems, rightItems);                 sortIfNeeded(rightItems); }
    @FXML private void onToLeft()     { moveSelected(rightList, rightItems, leftItems); sortIfNeeded(leftItems); }
    @FXML private void onAllToLeft()  { moveAll(rightItems, leftItems);                 sortIfNeeded(leftItems); }
    @FXML private void onStart()  { startJob(); }

//    private void startJob() {
//
//        //Schleife über alle Tabellen der Auswahlliste:
//        if (rightList.getItems().isEmpty()) {
//            Output.appendText("Bitte mindestens eine Tabellen auswählen!\n");
//            return;
//        } else {
//            Progress.setVisible(true);
//            InfoLabel.setVisible(true);
//            File file= new File(directoryField.getText());
//
//
//            rightList.getItems().forEach(item ->
//                    {
//                        String size="";
//                        size=getSize(item);
//                        Output.appendText(item + "\n");
//                        InfoLabel.setText("Exportiere " + item);
//                        logger.debug("Start Export " + item + " in File " + file.getAbsolutePath());
//                        Output.appendText("Start Export " + item + " in File " + file.getAbsolutePath() + "\n");
//                        Output.appendText("Tabellengröße: " + size + " GB");
//                        new Thread(() -> exportToZipAsync(item, file.getAbsolutePath(), Output, Progress)).start();
//
//                    }
//            );
//        }
//
//        Progress.setVisible(false);
//        InfoLabel.setVisible(false);
//
//    }
private void startJob() {
    if (rightList.getItems().isEmpty()) {
        Output.appendText("Bitte mindestens eine Tabelle auswählen!\n");
        return;
    }
    File dir = new File(directoryField.getText());
    if (!dir.isDirectory()) {
        Output.appendText("Bitte gültiges Export-Verzeichnis angeben!\n");
        return;
    }

    // Sichtbarkeit für die gesamte Queue-Phase steuern
    Progress.setManaged(true);
    Progress.setVisible(true);
    InfoLabel.setManaged(true);
    InfoLabel.setVisible(true);

    // Kopie der Ziele in eine Queue
    java.util.Queue<String> queue = new java.util.LinkedList<>(rightList.getItems());

    // Queue anwerfen
    runQueue(queue, dir.getAbsolutePath());
}

    // -------- Queue-Steuerung: startet den nächsten Task, bis leer --------
    private void runQueue(java.util.Queue<String> queue, String targetDir) {
        if (queue.isEmpty()) {
            // Fertig
            Progress.progressProperty().unbind();
            InfoLabel.textProperty().unbind();
            Progress.setVisible(false);
            Progress.setManaged(false);
            InfoLabel.setVisible(false);
            InfoLabel.setManaged(false);
            Output.appendText("Alle Exporte abgeschlossen.\n");
            return;
        }

        String table = queue.poll();

        // Task für diese Tabelle bauen
        Task<Void> task = createExportTask(table, targetDir, Output);

        // UI-Bindings für diesen Task
        Progress.progressProperty().bind(task.progressProperty());
        InfoLabel.textProperty().bind(task.messageProperty());

        // Beim Ende: Bindings lösen und den nächsten starten
        Runnable next = () -> {
            Progress.progressProperty().unbind();
            InfoLabel.textProperty().unbind();
            runQueue(queue, targetDir);
        };
        task.setOnSucceeded(e -> next.run());
        task.setOnFailed(e -> {
            Output.appendText("Fehler beim Export von " + table + ": " +
                    (task.getException() != null ? task.getException().getMessage() : "unbekannt") + "\n");
            next.run();
        });
        task.setOnCancelled(e -> next.run());

        // Starten
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    // -------- Task-Factory: baut den Task für genau EINE Tabelle --------
    private Task<Void> createExportTask(final String tableName, final String targetDir, final TextArea logArea) {
        final String zipPath = targetDir + File.separator + tableName + ".zip";
        final int total = Optional.ofNullable(getCountRows(tableName)).filter(i -> i > 0).orElse(-1);

        return new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                updateMessage("Starte: " + tableName);

                try (
                        Statement stmt = oracleConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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

                    if (total <= 0) updateProgress(-1, -1);

                    int rowIndex = 0;
                    int tick = 0;

                    while (rs.next()) {
                        rowIndex++; tick++;

                        for (int i = 1; i <= columnCount; i++) {
                            int type = meta.getColumnType(i);
                            String colName = meta.getColumnName(i);
                            if (type == java.sql.Types.BLOB) {
                                Blob blob = rs.getBlob(i);
                                if (blob != null) {
                                    byte[] data = blob.getBytes(1, (int) blob.length()); // TODO: streamen für große BLOBs
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

                        if (total > 0) updateProgress(rowIndex, total);
                        if (tick >= 10) {
                            tick = 0;
                            updateMessage(tableName + " -> Zeile: " + rowIndex);
                        }
                    }

                    csvWriter.flush();
                    zos.putNextEntry(new ZipEntry("export.csv"));
                    zos.write(csvStream.toByteArray());
                    zos.closeEntry();

                    final int finalRowCount = rowIndex;
                    Platform.runLater(() ->
                            logArea.appendText(tableName + " – exportierte Zeilen: " + finalRowCount + "\n")
                    );
                } catch (Exception e) {
                    Platform.runLater(() ->
                            logArea.appendText("Fehler beim Export von " + tableName + ": " + e.getMessage() + "\n")
                    );
                    throw e; // damit setOnFailed feuert
                }
                return null;
            }
        };
    }


    private final ObservableList<String> leftItems  = FXCollections.observableArrayList();
    private final ObservableList<String> rightItems = FXCollections.observableArrayList();

    private static final boolean AUTO_SORT = true;
    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        @Override public int compare(String a, String b) {
            return a.compareToIgnoreCase(b);
        }
    };

    private static final DataFormat DND_FORMAT = new DataFormat("application/x-duallist-items");


    @FXML
    public void initialize() {

        logger.debug("Connecting to database..." + DbConfig.getUrl());
        Stage stage = Main.getPrimaryStage();
        directoryField.setText("C:\\temp");

        directoryField.addEventHandler(MouseEvent.MOUSE_CLICKED, e -> {
            if (e.getClickCount() == 2) { // nur bei Doppelklick
                DirectoryChooser chooser = new DirectoryChooser();
                chooser.setTitle("Export Verzeichnis auswählen");

                // Optional: Startverzeichnis setzen
                File defaultDir = new File("C:\\temp");
                if (defaultDir.exists()) {
                    chooser.setInitialDirectory(defaultDir);
                }

                File selectedDirectory = chooser.showDialog(stage);
                if (selectedDirectory != null) {
                    directoryField.setText(selectedDirectory.getAbsolutePath());
                }
            }
        });


        new Thread(() -> getTablesAsync(leftItems)).start();

//        try {
//            oracleConn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());

            //refresh();
            //initTable();

      //      List<String> tables = fetchTableNames();
      //      if (tables.isEmpty()) {
      //          System.out.println("Keine Tabellen gefunden - Die Datenbank enthält keine benutzerdefinierten Tabellen.");
      //          return;
      //      }
      //      leftItems.addAll(tables);



//            ChoiceDialog<String> dialog = new ChoiceDialog<>(tables.get(0), tables);
//            dialog.setTitle("Tabelle wählen");
//            dialog.setHeaderText("Welche Tabelle möchtest du exportieren?");
//            dialog.setContentText("Tabelle:");
//
//            dialog.showAndWait().ifPresent(selectedTable -> {
//                FileChooser fc = new FileChooser();
//                fc.setTitle("ZIP-Datei für Export wählen");
//                fc.setInitialFileName("export_" + selectedTable + ".zip");
//                fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("ZIP-Dateien", "*.zip"));
//                File file = fc.showSaveDialog(stage);
//                if (file != null) {
//                    Progress.setVisible(true);
//                    InfoLabel.setText("Exportiere " + selectedTable);
//                    InfoLabel.setVisible(true);
//                    logger.debug("Start Export " + selectedTable + " in File " + file.getAbsolutePath());
//                    Output.appendText("Start Export " + selectedTable + " in File " + file.getAbsolutePath() + "\n");
//                    Output.appendText("Tabellengröße: " + getSize(selectedTable) + "GB");
//                    //new Thread(() -> exportToZip(selectedTable, file.getAbsolutePath(), Output, null)).start();
//                    new Thread(() -> exportToZipAsync(selectedTable, file.getAbsolutePath(), Output, Progress)).start();
//                }
//            });

  //      } catch (Exception e) {
  //          e.printStackTrace();
  //      }



        Progress.setVisible(false);
        InfoLabel.setVisible(false);

        leftList.setItems(leftItems);
        rightList.setItems(rightItems);

        // Multi-Select
        leftList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        rightList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        // Zellen (optional)
        leftList.setCellFactory(lv -> new SimpleCell());
        rightList.setCellFactory(lv -> new SimpleCell());

        // Doppelklick: einzelnes Item verschieben
        leftList.addEventHandler(MouseEvent.MOUSE_CLICKED, e -> {
            if (e.getClickCount() == 2) {
                String sel = leftList.getSelectionModel().getSelectedItem();
                if (sel != null) { moveOne(sel, leftItems, rightItems); sortIfNeeded(rightItems); }
            }
        });
        rightList.addEventHandler(MouseEvent.MOUSE_CLICKED, e -> {
            if (e.getClickCount() == 2) {
                String sel = rightList.getSelectionModel().getSelectedItem();
                if (sel != null) { moveOne(sel, rightItems, leftItems); sortIfNeeded(leftItems); }
            }
        });

        // Drag & Drop
        installDrag(leftList);
        installDrag(rightList);
        installDrop(leftList, leftItems, rightItems);
        installDrop(rightList, rightItems, leftItems);

        // Startlisten sortieren (optional)
        sortIfNeeded(leftItems);
        sortIfNeeded(rightItems);



    }

    private void getTablesAsync(ObservableList<String> leftItems) {

        Progress.setProgress(ProgressBar.INDETERMINATE_PROGRESS);
        Progress.setVisible(true);
        InfoLabel.setText("Hole Tabellen aus der Datenbank");
        InfoLabel.setVisible(true);
        logger.debug("Holen der Tabellen aus Datenbank");

        try {
            oracleConn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());

        List<String> tables = fetchTableNames();
        if (tables.isEmpty()) {
            System.out.println("Keine Tabellen gefunden - Die Datenbank enthält keine benutzerdefinierten Tabellen.");
            return;
        }
        leftItems.addAll(tables);
        Progress.setVisible(false);
        InfoLabel.setVisible(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    private String getSize(String selectedTable) {
//        String sizeInfo="0";
//
//        String sql="SELECT ROUND(SUM(bytes)/1024/1024/1024, 2) AS GB FROM user_segments s inner join user_lobs l on s.segment_name=l.segment_name and table_name='" + selectedTable + "'";
//        logger.debug("Ausführen SQL: " + sql);
//        try (PreparedStatement ps = oracleConn.prepareStatement(sql);
//             ResultSet rs = ps.executeQuery()) {
//            while (rs.next()) {
//                sizeInfo = rs.getString(1);
//            }
//        } catch (SQLException e) {
//            Platform.runLater(() -> Output.appendText("Fehler beim ermitteln der Tabellengröße: " + e.getMessage() + "\n"));
//        }
//
//        return sizeInfo;
//    }

    private String getSize(String table) {

        String sql="SELECT ROUND(SUM(bytes)/1024/1024/1024, 2) AS GB FROM user_segments s inner join user_lobs l on s.segment_name=l.segment_name and table_name= ?";

        logger.debug("Ausführen SQL: {} (param={})", sql, table);
        try (PreparedStatement ps = oracleConn.prepareStatement(sql)) {
            ps.setString(1, table.toUpperCase(Locale.ROOT)); // Oracle-Objektnamen sind i. d. R. UPPERCASE
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String val = rs.getString(1);
                    return (val == null || val.isBlank()) ? "0" : val; // extra-Schutz
                }
            }
        } catch (SQLException e) {
            Platform.runLater(() ->
                    Output.appendText("Fehler beim Ermitteln der Tabellengröße: " + e.getMessage() + "\n"));
        }
        return "0";
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

    public void exportToZipAsync(String tableName, String Path, TextArea logArea, ProgressBar progressBar) {

        Integer countRows = getCountRows(tableName);
        final int total = (countRows != null && countRows > 0) ? countRows : -1;

        String zipPath=Path+"\\"+tableName+".zip";

        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                try (
                        Statement stmt = oracleConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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

                    if (total <= 0) updateProgress(-1, -1); // indeterminate

                    while (rs.next()) {
                        rowIndex++;
                        tick++;

                        for (int i = 1; i <= columnCount; i++) {
                            int type = meta.getColumnType(i);
                            String colName = meta.getColumnName(i);
                            if (type == java.sql.Types.BLOB) {
                                Blob blob = rs.getBlob(i);
                                if (blob != null) {
                                    byte[] data = blob.getBytes(1, (int) blob.length()); // optional: streamen
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

                        if (total > 0) updateProgress(rowIndex, total);
                        if (tick >= 10) {
                            tick = 0;
                            updateMessage(tableName + " -> Zeile: " + rowIndex); // ✅ statt TableName.setText(...)
                        }
                    }

                    csvWriter.flush();
                    zos.putNextEntry(new ZipEntry("export.csv"));
                    zos.write(csvStream.toByteArray());
                    zos.closeEntry();

                    int finalRowCount = rowIndex;
                    Platform.runLater(() -> {
                        logArea.appendText(tableName + " Anzahl exportierte Zeilen: " + finalRowCount + "\n");
                    });
                } catch (Exception e) {
                    Platform.runLater(() -> logArea.appendText("\nFehler beim Export: " + e.getMessage() + "\n"));
                }
                return null;
            }
        };

        // UI-Bindings & Sichtbarkeit
        progressBar.progressProperty().bind(task.progressProperty());
        InfoLabel.textProperty().bind(task.messageProperty());

        task.setOnRunning(e -> {
            progressBar.setManaged(true);
            progressBar.setVisible(true);
            InfoLabel.setManaged(true);
            InfoLabel.setVisible(true);
        });
        Runnable hide = () -> {
            progressBar.progressProperty().unbind();
            InfoLabel.textProperty().unbind();
            progressBar.setVisible(false);
            progressBar.setManaged(false);
            InfoLabel.setVisible(false);
            InfoLabel.setManaged(false);
        };
        task.setOnSucceeded(e -> hide.run());
        task.setOnFailed(e -> hide.run());
        task.setOnCancelled(e -> hide.run());

        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }



    // --- Drag helpers ---
    private void installDrag(final ListView<String> list) {
        list.setOnDragDetected(e -> {
            List<String> selected = new ArrayList<String>(list.getSelectionModel().getSelectedItems());
            if (selected.isEmpty()) return;

            javafx.scene.input.Dragboard db = list.startDragAndDrop(TransferMode.MOVE);
            ClipboardContent content = new ClipboardContent();
            content.put(DND_FORMAT, joinPayload(selected));
            db.setContent(content);

            // kleine Vorschau (optional)
            ListCell<?> cell = (ListCell<?>) list.lookup(".list-cell");
            if (cell != null) {
                db.setDragView(cell.snapshot(null, null));
            }
            e.consume();
        });
    }

    private void installDrop(final ListView<String> targetList,
                             final ObservableList<String> targetItems,
                             final ObservableList<String> sourceItems) {

        targetList.setOnDragOver(e -> {
            if (e.getGestureSource() != targetList && e.getDragboard().hasContent(DND_FORMAT)) {
                e.acceptTransferModes(TransferMode.MOVE);
            }
            e.consume();
        });

        targetList.setOnDragDropped(e -> {
            javafx.scene.input.Dragboard db = e.getDragboard();
            boolean success = false;
            if (db.hasContent(DND_FORMAT)) {
                String payload = (String) db.getContent(DND_FORMAT);
                List<String> items = splitPayload(payload);
                moveList(items, sourceItems, targetItems);
                sortIfNeeded(targetItems);
                success = true;
            }
            e.setDropCompleted(success);
            e.consume();
        });
    }

    // --- Move/Sort utils ---
    private static void moveOne(String item, ObservableList<String> from, ObservableList<String> to) {
        if (from.remove(item)) to.add(item);
    }

    private static void moveSelected(ListView<String> fromList,
                                     ObservableList<String> from,
                                     ObservableList<String> to) {
        List<String> selected = new ArrayList<String>(fromList.getSelectionModel().getSelectedItems());
        if (selected.isEmpty()) return;
        moveList(selected, from, to);
        fromList.getSelectionModel().clearSelection();
    }

    private static void moveAll(ObservableList<String> from, ObservableList<String> to) {
        if (from.isEmpty()) return;
        to.addAll(from);
        from.clear();
    }

    private static void moveList(List<String> items,
                                 ObservableList<String> from,
                                 ObservableList<String> to) {
        from.removeAll(items);
        for (String s : items) {
            if (!to.contains(s)) {
                to.add(s);
            }
        }
    }

    private static void sortIfNeeded(ObservableList<String> list) {
        if (AUTO_SORT) {
            FXCollections.sort(list, COMPARATOR);
        }
    }

    private static String joinPayload(List<String> items) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append('\n');
            sb.append(items.get(i));
        }
        return sb.toString();
    }

    private static List<String> splitPayload(String payload) {
        if (payload == null || payload.isEmpty()) return java.util.Collections.emptyList();
        Set<String> set = new LinkedHashSet<String>(Arrays.asList(payload.split("\\R")));
        return new ArrayList<String>(set);
    }

    private static class SimpleCell extends ListCell<String> {
        @Override protected void updateItem(String item, boolean empty) {
            super.updateItem(item, empty);
            setText(empty ? null : item);
        }
    }




}
