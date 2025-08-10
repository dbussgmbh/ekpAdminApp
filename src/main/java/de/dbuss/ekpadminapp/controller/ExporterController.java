package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.Main;
import de.dbuss.ekpadminapp.util.DbConfig;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.*;
import javafx.beans.value.ChangeListener;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.DataFormat;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.TransferMode;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ExporterController {

    private static final Logger logger = LoggerFactory.getLogger(ExporterController.class);

    // --- DB ---
    private static Connection oracleConn;

    // --- Dual-List (IDs müssen in der FXML existieren) ---
    @FXML private ListView<String> leftList;
    @FXML private ListView<String> rightList;
    @FXML private Button toRight;
    @FXML private Button allToRight;
    @FXML private Button toLeft;
    @FXML private Button allToLeft;

    private final ObservableList<String> leftItems  = FXCollections.observableArrayList();
    private final ObservableList<String> rightItems = FXCollections.observableArrayList();

    private static final boolean AUTO_SORT = true;
    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        @Override public int compare(String a, String b) { return a.compareToIgnoreCase(b); }
    };
    private static final DataFormat DND_FORMAT = new DataFormat("application/x-duallist-items");

    // --- Verzeichnis + Start ---
    @FXML private TextField directoryField;   // Doppelklick -> DirectoryChooser
    @FXML private Button btnStart;

    // --- Parallelen-Spinner ---
    @FXML private Spinner<Integer> spnMaxParallels;

    // --- Globaler Fortschritt + Info ---
    @FXML private ProgressBar Progress;
    @FXML private Label InfoLabel;

    // --- TableView für Jobs ---
    @FXML private TableView<ExportItem> jobTable;
    @FXML private TableColumn<ExportItem, String>  colTable;
    @FXML private TableColumn<ExportItem, String>  colSize;
    @FXML private TableColumn<ExportItem, Number>  colRows;
    @FXML private TableColumn<ExportItem, String>  colBytes;
    @FXML private TableColumn<ExportItem, Double>  colProgress; // WICHTIG: Double!
    @FXML private TableColumn<ExportItem, String>  colStatus;
    @FXML private TableColumn<ExportItem, String>  colMessage;

    private final ObservableList<ExportItem> jobData = FXCollections.observableArrayList();
    private final Map<String, ExportItem> itemsByTable = new HashMap<String, ExportItem>();

    // --- Globale Byte-Zähler für Gesamtfortschritt ---
    private final AtomicLong bytesDone = new AtomicLong(0);
    private long totalBytesAll = 0L;

    // ----------------------------------------
    // Initialisierung
    // ----------------------------------------
    @FXML
    public void initialize() {
        // DB verbinden
        try {
            oracleConn = DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
            logger.debug("DB connected: {}", DbConfig.getUrl());
        } catch (SQLException e) {
            showError("DB-Verbindung fehlgeschlagen: " + e.getMessage());
        }

        // Default-Verzeichnis + Doppelklick für DirectoryChooser
        directoryField.setText(System.getProperty("user.home"));
        final Stage stage = Main.getPrimaryStage();
        directoryField.addEventHandler(MouseEvent.MOUSE_CLICKED, new javafx.event.EventHandler<MouseEvent>() {
            @Override public void handle(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    DirectoryChooser chooser = new DirectoryChooser();
                    chooser.setTitle("Export-Verzeichnis auswählen");
                    File def = new File(directoryField.getText());
                    if (def.isDirectory()) chooser.setInitialDirectory(def);
                    File selected = chooser.showDialog(stage);
                    if (selected != null) directoryField.setText(selected.getAbsolutePath());
                }
            }
        });

        // Spinner (1..#Kerne, Default=min(4, Kerne))
        int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
        SpinnerValueFactory.IntegerSpinnerValueFactory vf =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(1, cores, Math.min(4, cores));
        spnMaxParallels.setValueFactory(vf);
        spnMaxParallels.setEditable(true);
        spnMaxParallels.focusedProperty().addListener((obs, was, is) -> {
            if (!is) { try { spnMaxParallels.increment(0); } catch (Exception ignore) {} }
        });

        // Progress & Info initial aus
        Progress.setVisible(false); Progress.setManaged(false);
        InfoLabel.setVisible(false); InfoLabel.setManaged(false);

        // Dual-List
        leftList.setItems(leftItems);
        rightList.setItems(rightItems);
        leftList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        rightList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        leftList.setCellFactory(lv -> new SimpleCell());
        rightList.setCellFactory(lv -> new SimpleCell());

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

        installDrag(leftList);  installDrag(rightList);
        installDrop(leftList, leftItems, rightItems);
        installDrop(rightList, rightItems, leftItems);

        // Tabellenliste laden (Background)
        new Thread(new Runnable() {
            @Override public void run() {
                final List<String> tables = fetchTableNames();
                Platform.runLater(new Runnable() {
                    @Override public void run() {
                        leftItems.setAll(tables);
                        sortIfNeeded(leftItems);
                    }
                });
            }
        }, "load-tables").start();

        // Job-Tabelle
        jobTable.setItems(jobData);
        colTable.setCellValueFactory(d -> d.getValue().tableProperty());
        colSize.setCellValueFactory(d -> d.getValue().sizeGbProperty());
        colRows.setCellValueFactory(d -> d.getValue().rowsProperty());
        colStatus.setCellValueFactory(d -> d.getValue().statusProperty());
        colMessage.setCellValueFactory(d -> d.getValue().messageProperty());

        // Bytes-Spalte: gebunden an bytesDone/bytesTotal (wird live aktualisiert)
        colBytes.setCellValueFactory(d ->
                Bindings.createStringBinding(
                        new java.util.concurrent.Callable<String>() {
                            @Override public String call() {
                                ExportItem it = d.getValue();
                                return String.format("%,d / %,d",
                                        it.bytesDoneProperty().get(),
                                        it.bytesTotalProperty().get());
                            }
                        },
                        d.getValue().bytesDoneProperty(),
                        d.getValue().bytesTotalProperty()
                )
        );

        // Fortschritt: Double + stabile Cell (null/negativ -> 0)
        colProgress.setCellValueFactory(d -> d.getValue().progressProperty().asObject());
        colProgress.setCellFactory(tc -> new TableCell<ExporterController.ExportItem, Double>() {
            private final ProgressBar bar = new ProgressBar(0);
            { bar.setMaxWidth(Double.MAX_VALUE); }
            @Override protected void updateItem(Double value, boolean empty) {
                super.updateItem(value, empty);
                if (empty) { setGraphic(null); return; }
                double v = (value == null || value.isNaN() || value < 0) ? 0.0 : Math.min(1.0, value);
                bar.setProgress(v);
                setGraphic(bar);
            }
        });

        // Refresh bei Änderungen
        jobData.addListener((ListChangeListener<? super ExportItem>) c -> jobTable.refresh());
    }

    // ----------------------------------------
    // UI-Handler
    // ----------------------------------------
    @FXML private void onToRight()    { moveSelected(leftList, leftItems, rightItems);  sortIfNeeded(rightItems); }
    @FXML private void onAllToRight() { moveAll(leftItems, rightItems);                 sortIfNeeded(rightItems); }
    @FXML private void onToLeft()     { moveSelected(rightList, rightItems, leftItems); sortIfNeeded(leftItems); }
    @FXML private void onAllToLeft()  { moveAll(rightItems, leftItems);                 sortIfNeeded(leftItems); }

    @FXML
    private void onStart() {
        Integer val = spnMaxParallels.getValue();
        int maxParallels = (val == null ? 1 : val.intValue());
        int tables = rightItems.size();
        if (tables == 0) { showInfo("Bitte mindestens eine Tabelle auswählen!"); return; }
        if (maxParallels > tables) maxParallels = tables;
        startJob(maxParallels);
    }

    // ----------------------------------------
    // Start + Scheduler
    // ----------------------------------------
    private void startJob(final int maxParallels) {
        File dir = new File(directoryField.getText());
        if (!dir.isDirectory()) { showInfo("Bitte gültiges Export-Verzeichnis angeben!"); return; }

        Progress.setManaged(true); Progress.setVisible(true);
        InfoLabel.setManaged(true); InfoLabel.setVisible(true);
        btnStart.setDisable(true); spnMaxParallels.setDisable(true);

        // Job-Zeilen anlegen
        jobData.clear(); itemsByTable.clear();
        final List<String> tables = new ArrayList<String>(rightItems);
        for (String t : tables) {
            ExportItem it = new ExportItem(t);
            it.statusProperty().set("Geplant");
            jobData.add(it);
            itemsByTable.put(t, it);
        }

        // Prefetch: COUNT(*) + Bytes je Tabelle (Background)
        new Thread(new Runnable() {
            @Override public void run() {
                final Map<String, Integer> countCache = new LinkedHashMap<String, Integer>();
                final Map<String, Long>    bytesCache = new LinkedHashMap<String, Long>();
                long bytesTotal = 0L;

                for (final String t : tables) {
                    int rows = safeGetCountRows(t);
                    long b   = getTableSizeBytes(t);
                    countCache.put(t, rows);
                    bytesCache.put(t, b);
                    bytesTotal += Math.max(b, 0L);

                    final int fRows = rows;
                    final long fBytes = b;
                    Platform.runLater(new Runnable() {
                        @Override public void run() {
                            ExportItem it = itemsByTable.get(t);
                            if (it != null) {
                                it.bytesTotalProperty().set(fBytes); // zuerst total
                                it.bytesDoneProperty().set(0);
                                it.rowsProperty().set(fRows);
                                it.sizeGbProperty().set(String.format(Locale.ROOT, "%.2f", fBytes / 1024.0 / 1024.0 / 1024.0));
                                it.statusProperty().set("Wartet");
                            }
                        }
                    });
                }

                final long totalB = bytesTotal;
                Platform.runLater(new Runnable() {
                    @Override public void run() {
                        bytesDone.set(0);
                        totalBytesAll = totalB;
                        Progress.setProgress(totalB > 0 ? 0 : ProgressBar.INDETERMINATE_PROGRESS);
                        InfoLabel.setText(String.format("Starte Exporte – Gesamt: %,d Bytes – parallel: %d", totalB, maxParallels));
                        runSchedulerWeightedBytes(tables, countCache, bytesCache, dir.getAbsolutePath(), maxParallels);
                    }
                });
            }
        }, "prefetch-meta").start();
    }

    private void runSchedulerWeightedBytes(final List<String> tables,
                                           final Map<String,Integer> countCache,
                                           final Map<String,Long> bytesCache,
                                           final String targetDir,
                                           final int maxParallels) {

        final ArrayDeque<String> queue = new ArrayDeque<String>(tables);
        final int totalJobs = tables.size();
        final int[] running = new int[]{0};
        final int[] doneJobs = new int[]{0};

        Runnable pump = new Runnable() {
            @Override public void run() {
                while (running[0] < maxParallels && !queue.isEmpty()) {
                    final String table = queue.pollFirst();
                    running[0]++;

                    final ExportItem row = itemsByTable.get(table);
                    if (row != null) { row.statusProperty().set("Läuft"); row.messageProperty().set("Wird gestartet…"); }

                    final int rows = Math.max(0, countCache.getOrDefault(table, 0));
                    final long tb  = Math.max(0L, bytesCache.getOrDefault(table, 0L));

                    Task<Void> task = createExportTaskBytes(table, rows, tb, targetDir, row);

                    task.setOnSucceeded(e -> {
                        doneJobs[0]++; running[0]--;
                        if (row != null) { row.statusProperty().set("Fertig"); row.messageProperty().set("OK"); }
                        if (doneJobs[0] == totalJobs) finishAll(); else Platform.runLater(this);
                    });

                    task.setOnFailed(e -> {
                        doneJobs[0]++; running[0]--;
                        Throwable ex = task.getException();
                        if (row != null) { row.statusProperty().set("Fehler"); row.messageProperty().set(ex != null ? ex.getMessage() : "unbekannt"); }
                        if (doneJobs[0] == totalJobs) finishAll(); else Platform.runLater(this);
                    });

                    new Thread(task, "export-" + table).start();
                }
            }

            private void finishAll() {
                InfoLabel.setText("Alle Exporte abgeschlossen.");
                Progress.setVisible(false); Progress.setManaged(false);
                InfoLabel.setVisible(false); InfoLabel.setManaged(false);
                btnStart.setDisable(false); spnMaxParallels.setDisable(false);
            }
        };

        Platform.runLater(pump);
    }

    // ----------------------------------------
    // Task-Factory (Byte-basiert, BLOB-Streaming) + detailliertes Logging
    // ----------------------------------------
    private Task<Void> createExportTaskBytes(final String tableName,
                                             final int tableCount,
                                             final long tableBytes,
                                             final String targetDir,
                                             final ExportItem row) {

        final String zipPath = targetDir + File.separator + tableName + ".zip";

        return new Task<Void>() {
            @Override
            protected Void call() throws Exception {

                long t0 = System.nanoTime();
                logger.info("[{}] START -> {}", tableName, zipPath);

                long localBlobBytes = 0L; // nur die in dieser Tabelle gestreamten BLOB-Bytes
                Path tmpCsv = Files.createTempFile("export_" + tableName + "_", ".csv");

                try (
                        Statement stmt = oracleConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                        FileOutputStream fos = new FileOutputStream(zipPath);
                        ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(fos));
                        BufferedWriter csvWriter = Files.newBufferedWriter(
                                tmpCsv, StandardCharsets.UTF_8,
                                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                ) {
                    try { stmt.setFetchSize(1000); } catch (Exception ignore) {}

                    // optional: Ordner-Entry für blobs/
                    zos.putNextEntry(new ZipEntry("blobs/"));
                    zos.closeEntry();

                    long tQ0 = System.nanoTime();
                    logger.info("[{}] executeQuery START", tableName);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                    long tQ1 = System.nanoTime();
                    logger.info("[{}] executeQuery DONE  (+{} ms total {} ms)", tableName, ms(tQ1 - tQ0), ms(tQ1 - t0));

                    long tM0 = System.nanoTime();
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();
                    long tM1 = System.nanoTime();
                    logger.info("[{}] readMeta DONE      (+{} ms total {} ms)", tableName, ms(tM1 - tM0), ms(tM1 - t0));

                    // Header in CSV (Temp)
                    for (int i = 1; i <= columnCount; i++) {
                        csvWriter.write(meta.getColumnName(i));
                        if (i < columnCount) csvWriter.write(",");
                    }
                    csvWriter.newLine();
                    csvWriter.flush();

                    if (row != null) Platform.runLater(() -> row.messageProperty().set("Export läuft..."));

                    int rowIndex = 0;
                    long tPrev = System.nanoTime();

                    long lastUiNs = System.nanoTime();
                    long bytesSinceUi = 0L;
                    final long UI_BYTE_STEP = 50L * 1024 * 1024;   // alle 50 MB
                    final long UI_TIME_STEP_NS = 1_000_000_000L;   // oder mind. jede 1 Sekunde


                    while (rs.next()) {
                        long tNow = System.nanoTime();
                        logger.debug("[{}] fetch row {}: +{} ms (total {} ms)",
                                tableName, rowIndex + 1, ms(tNow - tPrev), ms(tNow - t0));
                        tPrev = tNow;

                        rowIndex++;

                        for (int i = 1; i <= columnCount; i++) {
                            int type = meta.getColumnType(i);
                            String colName = meta.getColumnName(i);

                            if (type == java.sql.Types.BLOB) {
                                Blob blob = rs.getBlob(i);
                                if (blob != null) {
                                    String blobPath = "blobs/blob_" + rowIndex + "_" + colName + ".bin";
                                    long added = copyBlobToZipCounted(zos, blob, blobPath);
                                    localBlobBytes += added;

                                    // Pfad in CSV
                                    csvWriter.write(blobPath);

                                    // Fortschritt (global/zeilen)
                                    long done = bytesDone.addAndGet(added);
                                    if (row != null) {
                                        long cur = row.bytesDoneProperty().get();
                                        long inc = added;
                                        Platform.runLater(() -> row.bytesDoneProperty().set(cur + inc));
                                    }

                                    // --- NEU: zeit-/bytebasierte Statusmeldung
                                    bytesSinceUi += added;
                                    long now = System.nanoTime();
                                    if (bytesSinceUi >= UI_BYTE_STEP || (now - lastUiNs) >= UI_TIME_STEP_NS) {
                                        final long doneF = done;
                                        final long totalF = totalBytesAll;
                                        final int rowF = rowIndex;
                                        Platform.runLater(() -> {
                                            if (row != null) {
                                                row.messageProperty().set(String.format(
                                                        "Export läuft… Zeile %d · %,d / %,d Bytes gesamt",
                                                        rowF, doneF, totalF));
                                            }
                                            if (totalF > 0) {
                                                double p = Math.min(1.0, doneF / (double) totalF);
                                                Progress.setProgress(p);
                                                InfoLabel.setText(String.format(
                                                        "Gesamt: %.1f%% (%,d / %,d Bytes)", p * 100.0, doneF, totalF));
                                            }
                                        });
                                        bytesSinceUi = 0L;
                                        lastUiNs = now;
                                    }
                                    // ---
                                } else {
                                    csvWriter.write("NULL");
                                }
                            } else {
                                Object value = rs.getObject(i);
                                String s = (value != null ? value.toString().replace("\n", " ") : "NULL");
                                csvWriter.write(s);
                            }
                            if (i < columnCount) csvWriter.write(",");
                        }
                        csvWriter.newLine();

                        if (row != null && rowIndex % 500 == 0) {
                            int idx = rowIndex;
                            Platform.runLater(() -> row.messageProperty().set(
                                    "Zeile " + idx + (tableCount > 0 ? " / " + tableCount : "")));
                        }
                    }

                    csvWriter.flush();

                    // Restbytes gutschreiben (non-LOB + CSV), damit Summe = tableBytes
                    if (tableBytes > 0 && localBlobBytes < tableBytes) {
                        long missing = tableBytes - localBlobBytes;
                        long done = bytesDone.addAndGet(missing);
                        logger.debug("[{}] settle non-LOB bytes: +{} (blob {} / seg {})",
                                tableName, missing, localBlobBytes, tableBytes);

                        if (row != null) {
                            long cur = row.bytesDoneProperty().get();
                            Platform.runLater(() -> row.bytesDoneProperty().set(cur + missing));
                        }
                        if (totalBytesAll > 0) {
                            double p = Math.min(1.0, done / (double) totalBytesAll);
                            Platform.runLater(() -> {
                                Progress.setProgress(p);
                                InfoLabel.setText(String.format(
                                        "Gesamt: %.1f%% (%,d / %,d Bytes)", p * 100.0, done, totalBytesAll));
                            });
                        }
                    }

                    long tZ0 = System.nanoTime();
                    zos.putNextEntry(new ZipEntry("export.csv"));
                    Files.copy(tmpCsv, zos);
                    zos.closeEntry();
                    long tZ1 = System.nanoTime();
                    logger.info("[{}] zipped CSV in {} ms (rows {})",
                            tableName, ms(tZ1 - tZ0), rowIndex);

                    final int finalRowCount = rowIndex;
                    if (row != null) Platform.runLater(() -> row.messageProperty().set("Fertig – Zeilen: " + finalRowCount));

                    long tEnd = System.nanoTime();
                    logger.info("[{}] DONE total {} ms", tableName, ms(tEnd - t0));

                } catch (Exception ex) {
                    long tErr = System.nanoTime();
                    logger.error("[{}] FAILED after {} ms: {}", tableName, ms(tErr - t0), ex.toString(), ex);
                    if (row != null) {
                        final String msg = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                        Platform.runLater(() -> { row.statusProperty().set("Fehler"); row.messageProperty().set(msg); });
                    }
                    throw ex;
                } finally {
                    try { Files.deleteIfExists(tmpCsv); } catch (IOException ignore) {}
                }

                return null;
            }
        };
    }

    // ----------------------------------------
    // Copy BLOB -> ZIP (Streaming) + zählt Bytes und liefert Summe zurück
    // ----------------------------------------
    private static long copyBlobToZipCounted(ZipOutputStream zos, Blob blob, String entryName)
            throws IOException, SQLException {
        long sum = 0L;
        zos.putNextEntry(new ZipEntry(entryName));
        InputStream in = null;
        try {
            in = blob.getBinaryStream();
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = in.read(buffer)) != -1) {
                zos.write(buffer, 0, read);
                sum += read;
            }
        } finally {
            if (in != null) try { in.close(); } catch (IOException ignore) {}
            zos.closeEntry();
            try { blob.free(); } catch (Throwable ignore) {}
        }
        return sum;
    }

    // ----------------------------------------
    // DB-Helfer
    // ----------------------------------------
    private int safeGetCountRows(String tableName) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (PreparedStatement ps = oracleConn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        } catch (SQLException e) {
            showError("COUNT(*) für " + tableName + " fehlgeschlagen: " + e.getMessage());
        }
        return 0;
    }

    private long getTableSizeBytes(String table) {
        final String sql =
                "SELECT NVL(SUM(bytes),0) AS BYTES " +
                        "FROM user_segments " +
                        "WHERE (segment_type IN ('TABLE','TABLE PARTITION') AND segment_name = ?) " +
                        "   OR (segment_type IN ('LOBSEGMENT','LOB PARTITION') " +
                        "       AND segment_name IN (SELECT segment_name FROM user_lobs WHERE table_name = ?))";
        try (PreparedStatement ps = oracleConn.prepareStatement(sql)) {
            String t = table.toUpperCase(Locale.ROOT);
            ps.setString(1, t);
            ps.setString(2, t);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        } catch (SQLException e) {
            showError("Größe (Bytes) für " + table + " fehlgeschlagen: " + e.getMessage());
        }
        return 0L;
    }

    private List<String> fetchTableNames() {
        List<String> tables = new ArrayList<String>();
        try (PreparedStatement ps = oracleConn.prepareStatement(
                "SELECT table_name FROM user_tables ORDER BY table_name");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            showError("Fehler beim Laden der Tabellen: " + e.getMessage());
        }
        return tables;
    }

    // ----------------------------------------
    // Dual-List Utilities
    // ----------------------------------------
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
    private static void moveList(List<String> items, ObservableList<String> from, ObservableList<String> to) {
        from.removeAll(items);
        for (String s : items) if (!to.contains(s)) to.add(s);
    }
    private static void sortIfNeeded(ObservableList<String> list) {
        if (AUTO_SORT) FXCollections.sort(list, COMPARATOR);
    }
    private void installDrag(final ListView<String> list) {
        list.setOnDragDetected(e -> {
            List<String> selected = new ArrayList<String>(list.getSelectionModel().getSelectedItems());
            if (selected.isEmpty()) return;
            javafx.scene.input.Dragboard db = list.startDragAndDrop(TransferMode.MOVE);
            ClipboardContent content = new ClipboardContent();
            content.put(DND_FORMAT, joinPayload(selected));
            db.setContent(content);
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

    // ----------------------------------------
    // Helper UI
    // ----------------------------------------
    private void showInfo(String msg) {
        InfoLabel.setManaged(true); InfoLabel.setVisible(true);
        InfoLabel.setText(msg);
    }
    private void showError(String msg) {
        logger.error(msg);
        Platform.runLater(() -> {
            Alert a = new Alert(Alert.AlertType.ERROR, msg, ButtonType.OK);
            a.setHeaderText("Fehler");
            a.showAndWait();
        });
    }

    // ----------------------------------------
    // Zeilenmodell für die TableView
    // ----------------------------------------
    public static class ExportItem {
        private final StringProperty table   = new SimpleStringProperty();
        private final StringProperty sizeGb  = new SimpleStringProperty("0.00");
        private final IntegerProperty rows   = new SimpleIntegerProperty(0);

        private final LongProperty bytesDone  = new SimpleLongProperty(0);
        private final LongProperty bytesTotal = new SimpleLongProperty(0);

        private final StringProperty status  = new SimpleStringProperty("Geplant");
        private final StringProperty message = new SimpleStringProperty("");

        private final ReadOnlyDoubleWrapper progress = new ReadOnlyDoubleWrapper(0);

        public ExportItem(String table) {
            this.table.set(table);
            ChangeListener<Number> recalc = (obs, o, n) -> {
                long done = bytesDone.get();
                long total = bytesTotal.get();
                double p = (total > 0) ? (double) done / (double) total : 0.0;
                progress.set(Math.max(0.0, Math.min(1.0, p))); // clamp 0..1
            };
            bytesDone.addListener(recalc);
            bytesTotal.addListener(recalc);
        }

        public StringProperty tableProperty()      { return table; }
        public StringProperty sizeGbProperty()     { return sizeGb; }
        public IntegerProperty rowsProperty()      { return rows; }
        public LongProperty bytesDoneProperty()    { return bytesDone; }
        public LongProperty bytesTotalProperty()   { return bytesTotal; }
        public StringProperty statusProperty()     { return status; }
        public StringProperty messageProperty()    { return message; }
        public ReadOnlyDoubleProperty progressProperty() { return progress.getReadOnlyProperty(); }
    }

    // ---- kleine Zeit-Helferfunktion (Millis aus ns) ----
    private static long ms(long nanos) { return nanos / 1_000_000L; }
}
