package de.dbuss.ekpadminapp.controller;

import de.dbuss.ekpadminapp.Main;
import de.dbuss.ekpadminapp.util.DbConfig;
import de.dbuss.ekpadminapp.util.SimpleCell;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ExporterController {

    private static final Logger logger = LoggerFactory.getLogger(ExporterController.class);

    // ---- Dual-List ----
    @FXML private ListView<String> leftList;
    @FXML private ListView<String> rightList;
    @FXML private Button toRight;
    @FXML private Button allToRight;
    @FXML private Button toLeft;
    @FXML private Button allToLeft;

    private final ObservableList<String> leftItems  = FXCollections.observableArrayList();
    private final ObservableList<String> rightItems = FXCollections.observableArrayList();

    private static final boolean AUTO_SORT = true;
    private static final Comparator<String> COMPARATOR = Comparator.comparing(String::toLowerCase);
    private static final DataFormat DND_FORMAT = new DataFormat("application/x-duallist-items");

    // ---- Verzeichnis + Start/Stop ----
    @FXML private TextField directoryField;
    @FXML private Button btnStart;
    @FXML private Button btnStop;
    @FXML private Spinner<Integer> spnMaxParallels;

    // ---- Globaler Fortschritt ----
    @FXML private ProgressBar Progress;
    @FXML private Label InfoLabel;

    // ---- Job-Tabelle ----
    @FXML private TableView<ExportItem> jobTable;
    @FXML private TableColumn<ExportItem, String>  colTable;
    @FXML private TableColumn<ExportItem, String>  colSize;
    @FXML private TableColumn<ExportItem, Number>  colRows;
    @FXML private TableColumn<ExportItem, String>  colBytes;
    @FXML private TableColumn<ExportItem, Double>  colProgress;
    @FXML private TableColumn<ExportItem, String>  colStatus;
    @FXML private TableColumn<ExportItem, String>  colMessage;
    @FXML private TableColumn<ExportItem, Void>    colAction; // Icon-Button „Abbrechen“

    private final ObservableList<ExportItem> jobData = FXCollections.observableArrayList();
    private final Map<String, ExportItem> itemsByTable = new HashMap<>();

    // Laufende Tasks/Statements (für Cancel pro Zeile / Stop-All)
    private final Map<String, Task<Void>> runningTasks = new ConcurrentHashMap<>();
    private final Map<String, Statement>  runningStmts = new ConcurrentHashMap<>();

    // Globaler Byte-Fortschritt
    private final AtomicLong bytesDone = new AtomicLong(0);
    private long totalBytesAll = 0L;

    // ---------------------------------------------------
    // Init
    // ---------------------------------------------------
    @FXML
    public void initialize() {
        // Default-Verzeichnis + Doppelklick -> DirectoryChooser
        directoryField.setText(System.getProperty("user.home"));
        final Stage stage = Main.getPrimaryStage();
        directoryField.addEventHandler(MouseEvent.MOUSE_CLICKED, e -> {
            if (e.getClickCount() == 2) {
                DirectoryChooser chooser = new DirectoryChooser();
                chooser.setTitle("Export-Verzeichnis auswählen");
                File def = new File(directoryField.getText());
                if (def.isDirectory()) chooser.setInitialDirectory(def);
                File selected = chooser.showDialog(stage);
                if (selected != null) directoryField.setText(selected.getAbsolutePath());
            }
        });

        // Spinner (1..#Kerne, default min(4, Kerne))
        int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
        spnMaxParallels.setValueFactory(
                new SpinnerValueFactory.IntegerSpinnerValueFactory(1, cores, Math.min(4, cores))
        );
        spnMaxParallels.setEditable(true);

        // Progress & Info initial aus
        Progress.setVisible(false); Progress.setManaged(false);
        InfoLabel.setVisible(false); InfoLabel.setManaged(false);
        if (btnStop != null) btnStop.setDisable(true);

        // Dual-List
        leftList.setItems(leftItems);
        rightList.setItems(rightItems);
        leftList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        rightList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        leftList.setCellFactory(lv -> new SimpleCell());
        rightList.setCellFactory(lv -> new SimpleCell());
        installDrag(leftList);  installDrag(rightList);
        installDrop(leftList, leftItems, rightItems);
        installDrop(rightList, rightItems, leftItems);

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

        // Tabellen laden (eigene Connection)
        new Thread(() -> {
            List<String> tables = fetchTableNames();
            Platform.runLater(() -> {
                leftItems.setAll(tables);
                sortIfNeeded(leftItems);
            });
        }, "load-tables").start();

        // Job-Tabelle
        jobTable.setItems(jobData);
        colTable.setCellValueFactory(d -> d.getValue().tableProperty());
        colSize.setCellValueFactory(d -> d.getValue().sizeGbProperty());
        colRows.setCellValueFactory(d -> d.getValue().rowsProperty());
        colStatus.setCellValueFactory(d -> d.getValue().statusProperty());
        colMessage.setCellValueFactory(d -> d.getValue().messageProperty());

        colBytes.setCellValueFactory(d ->
                Bindings.createStringBinding(
                        () -> String.format("%,d / %,d",
                                d.getValue().bytesDoneProperty().get(),
                                d.getValue().bytesTotalProperty().get()),
                        d.getValue().bytesDoneProperty(),
                        d.getValue().bytesTotalProperty()
                )
        );

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

        // Icon-Button „Abbrechen“ je Zeile
        if (colAction != null) {
            colAction.setCellFactory(tc -> new TableCell<ExporterController.ExportItem, Void>() {

                private final Button btn = new Button();
                private ExportItem boundItem;

                {
                    Label icon = new Label("\u2716"); // ✖
                    btn.setGraphic(icon);
                    btn.setTooltip(new Tooltip("Diesen Export abbrechen"));
                    btn.getStyleClass().add("danger-icon-button"); // optional via CSS
                    btn.setMinWidth(32);
                    btn.setOnAction(e -> {
                        ExportItem item = (boundItem != null) ? boundItem : getTableView().getItems().get(getIndex());
                        if (item != null) cancelJob(item.getTable());
                    });
                }

                private final javafx.beans.value.ChangeListener<String> statusListener = (obs, o, n) ->
                        btn.setDisable(n == null || !"Läuft".equalsIgnoreCase(n));

                @Override protected void updateItem(Void v, boolean empty) {
                    super.updateItem(v, empty);
                    if (empty) {
                        setGraphic(null);
                        if (boundItem != null) boundItem.statusProperty().removeListener(statusListener);
                        boundItem = null;
                        return;
                    }
                    ExportItem it = getTableView().getItems().get(getIndex());
                    if (boundItem != null && boundItem != it) {
                        boundItem.statusProperty().removeListener(statusListener);
                    }
                    boundItem = it;
                    btn.setDisable(it == null || !"Läuft".equalsIgnoreCase(it.getStatus()));
                    if (it != null) it.statusProperty().addListener(statusListener);
                    setGraphic(btn);
                }
            });
        }

        // Refresh on changes
        jobData.addListener((ListChangeListener<? super ExportItem>) c -> jobTable.refresh());
    }

    // ---------------------------------------------------
    // UI-Handler
    // ---------------------------------------------------
    @FXML private void onToRight()    { moveSelected(leftList, leftItems, rightItems);  sortIfNeeded(rightItems); }
    @FXML private void onAllToRight() { moveAll(leftItems, rightItems);                 sortIfNeeded(rightItems); }
    @FXML private void onToLeft()     { moveSelected(rightList, rightItems, leftItems); sortIfNeeded(leftItems); }
    @FXML private void onAllToLeft()  { moveAll(rightItems, leftItems);                 sortIfNeeded(leftItems); }

    @FXML
    private void onStart() {
        Integer val = spnMaxParallels.getValue();
        int maxParallels = (val == null ? 1 : val);
        if (rightItems.isEmpty()) {
            showInfo("Bitte mindestens eine Tabelle auswählen!");
            return;
        }
        if (maxParallels > rightItems.size()) maxParallels = rightItems.size();
        startJob(maxParallels);
    }

    @FXML
    private void onStopAll() {
        // Tasks abbrechen
        for (Task<Void> t : runningTasks.values()) {
            if (t != null && t.isRunning()) t.cancel(true);
        }
        // Statements canceln
        for (Statement s : runningStmts.values()) {
            if (s != null) try { s.cancel(); } catch (SQLException ignore) {}
        }
        InfoLabel.setText("Abbruch angefordert …");
        if (btnStop != null) btnStop.setDisable(true);
    }

    // ---------------------------------------------------
    // Start + Scheduler
    // ---------------------------------------------------
    private void startJob(final int maxParallels) {
        File dir = new File(directoryField.getText());
        if (!dir.isDirectory()) { showInfo("Bitte gültiges Export-Verzeichnis angeben!"); return; }

        Progress.setManaged(true); Progress.setVisible(true);
        InfoLabel.setManaged(true); InfoLabel.setVisible(true);
        btnStart.setDisable(true);
        if (btnStop != null) btnStop.setDisable(false);
        spnMaxParallels.setDisable(true);

        // Job-Zeilen anlegen
        jobData.clear(); itemsByTable.clear();
        final List<String> tables = new ArrayList<>(rightItems);
        for (String t : tables) {
            ExportItem it = new ExportItem(t);
            it.setStatus("Geplant");
            jobData.add(it);
            itemsByTable.put(t, it);
        }

        // Prefetch Metadaten (eigene Connections)
        new Thread(() -> {
            Map<String, Integer> countCache = new LinkedHashMap<>();
            Map<String, Long>    bytesCache = new LinkedHashMap<>();
            long bytesTotal = 0L;
            for (String t : tables) {
                int rows = safeGetCountRows(t);
                long b   = getTableSizeBytes(t);
                countCache.put(t, rows);
                bytesCache.put(t, b);
                bytesTotal += Math.max(0L, b);

                final int fRows = rows;
                final long fBytes = b;
                Platform.runLater(() -> {
                    ExportItem it = itemsByTable.get(t);
                    if (it != null) {
                        it.setRows(fRows);
                        it.setBytesTotal(fBytes);
                        it.setSizeGb(String.format(Locale.ROOT, "%.2f", fBytes / 1024.0 / 1024.0 / 1024.0));
                        it.setStatus("Wartet");
                    }
                });
            }
            final long totalB = bytesTotal;
            Platform.runLater(() -> {
                bytesDone.set(0);
                totalBytesAll = totalB;
                Progress.setProgress(totalB > 0 ? 0 : ProgressBar.INDETERMINATE_PROGRESS);
                InfoLabel.setText(String.format("Starte Exporte – Gesamt: %,d Bytes – parallel: %d", totalB, maxParallels));
                runSchedulerWeighted(tables, countCache, bytesCache, dir.getAbsolutePath(), maxParallels);
            });
        }, "prefetch-meta").start();
    }

    private void runSchedulerWeighted(final List<String> tables,
                                      final Map<String,Integer> countCache,
                                      final Map<String,Long> bytesCache,
                                      final String targetDir,
                                      final int maxParallels) {
        final ArrayDeque<String> queue = new ArrayDeque<>(tables);
        final int totalJobs = tables.size();
        final int[] running = {0};
        final int[] doneJobs = {0};

        Runnable pump = new Runnable() {
            @Override public void run() {
                while (running[0] < maxParallels && !queue.isEmpty()) {
                    final String table = queue.pollFirst();
                    running[0]++;

                    final ExportItem row = itemsByTable.get(table);
                    if (row != null) { row.setStatus("Läuft"); row.setMessage("Wird gestartet…"); }

                    final int rows = Math.max(0, countCache.getOrDefault(table, 0));
                    final long tb  = Math.max(0L, bytesCache.getOrDefault(table, 0L));

                    Task<Void> task = createExportTask(table, rows, tb, targetDir, row);

                    runningTasks.put(table, task);

                    task.setOnSucceeded(e -> {
                        runningTasks.remove(table);
                        running[0]--; doneJobs[0]++;
                        // keine Überschreibung der finalen Message/Status – Task hat sie gesetzt
                        if (doneJobs[0] == totalJobs) finishAll(); else Platform.runLater(this);
                    });
                    task.setOnFailed(e -> {
                        runningTasks.remove(table);
                        running[0]--; doneJobs[0]++;
                        Throwable ex = task.getException();
                        if (row != null) {
                            row.setStatus("Fehler");
                            row.setMessage(ex != null ? ex.getMessage() : "unbekannt");
                        }
                        if (doneJobs[0] == totalJobs) finishAll(); else Platform.runLater(this);
                    });
                    task.setOnCancelled(e -> {
                        runningTasks.remove(table);
                        running[0]--; doneJobs[0]++;
                        if (row != null) {
                            row.setStatus("Abgebrochen");
                            row.setMessage("Export abgebrochen");
                        }
                        if (doneJobs[0] == totalJobs) finishAll(); else Platform.runLater(this);
                    });

                    new Thread(task, "export-" + table).start();
                }
            }

            private void finishAll() {
                InfoLabel.setText("Alle Exporte abgeschlossen.");
                Progress.setVisible(false); Progress.setManaged(false);
                InfoLabel.setVisible(false); InfoLabel.setManaged(false);
                btnStart.setDisable(false);
                spnMaxParallels.setDisable(false);
                if (btnStop != null) btnStop.setDisable(true);
            }
        };

        Platform.runLater(pump);
    }

    // ---------------------------------------------------
    // Export-Task (Streaming, Cancel, MB/s, ETA & Laufzeit)
    // ---------------------------------------------------
    private Task<Void> createExportTask(final String tableName,
                                        final int tableRowCount,
                                        final long tableBytes,
                                        final String targetDir,
                                        final ExportItem row) {

        final String zipPath = targetDir + File.separator + tableName + ".zip";

        return new Task<>() {
            @Override
            protected Void call() throws Exception {
                long t0 = System.nanoTime();
                long lastUiNs = t0;
                long bytesSinceUi = 0L;
                long tableDone = 0L;

                final long UI_BYTE_STEP   = 20L * 1024 * 1024; // 20 MB
                final long UI_TIME_STEPNS = 1_000_000_000L;    // 1 s

                Path tmpCsv = Files.createTempFile("export_" + tableName + "_", ".csv");

                try (Connection conn = newConnection();
                     Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                     FileOutputStream fos = new FileOutputStream(zipPath);
                     ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(fos));
                     BufferedWriter csvWriter = Files.newBufferedWriter(
                             tmpCsv, StandardCharsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                ) {
                    runningStmts.put(tableName, stmt);
                    try { stmt.setFetchSize(1000); } catch (Exception ignore) {}

                    // blobs/ Ordner
                    zos.putNextEntry(new ZipEntry("blobs/"));
                    zos.closeEntry();

                    long tQ0 = System.nanoTime();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                    long tQ1 = System.nanoTime();
                    logger.info("[{}] query done in {} ms", tableName, ms(tQ1 - tQ0));

                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    // CSV Header
                    for (int i = 1; i <= columnCount; i++) {
                        csvWriter.write(meta.getColumnName(i));
                        if (i < columnCount) csvWriter.write(",");
                    }
                    csvWriter.newLine();
                    csvWriter.flush();

                    if (row != null) Platform.runLater(() -> row.setMessage("Export läuft…"));

                    int rowIndex = 0;
                    while (rs.next()) {
                        if (isCancelled()) {
                            logger.warn("[{}] cancelled before row {}", tableName, rowIndex + 1);
                            break;
                        }
                        rowIndex++;

                        for (int i = 1; i <= columnCount; i++) {
                            if (isCancelled()) break;

                            int type = meta.getColumnType(i);
                            String colName = meta.getColumnName(i);

                            if (type == Types.BLOB) {
                                Blob blob = rs.getBlob(i);
                                if (blob != null) {
                                    String blobPath = "blobs/blob_" + rowIndex + "_" + colName + ".bin";
                                    long tB0 = System.nanoTime();
                                    long added = copyBlobToZipCounted(zos, blob, blobPath, this);
                                    long tB1 = System.nanoTime();
                                    tableDone += added;
                                    bytesSinceUi += added;

                                    // CSV schreibt den Pfad
                                    csvWriter.write(blobPath);

                                    long doneGlobal = bytesDone.addAndGet(added);
                                    if (row != null) {
                                        long cur = row.getBytesDone();
                                        long inc = added;
                                        Platform.runLater(() -> row.setBytesDone(cur + inc));
                                    }

                                    long now = System.nanoTime();
                                    if (bytesSinceUi >= UI_BYTE_STEP || (now - lastUiNs) >= UI_TIME_STEPNS) {
                                        double seconds = (now - lastUiNs) / 1_000_000_000.0;
                                        double bps  = seconds > 0 ? (bytesSinceUi / seconds) : 0.0;
                                        double mbps = bps / (1024.0 * 1024.0);

                                        String etaTableStr = "—";
                                        if (tableBytes > 0 && bps > 0) {
                                            long left = Math.max(0L, tableBytes - tableDone);
                                            long etaS = (long) Math.ceil(left / bps);
                                            etaTableStr = fmtDuration(etaS);
                                        }

                                        String etaGlobalStr = "—";
                                        if (totalBytesAll > 0 && bps > 0) {
                                            long leftG = Math.max(0L, totalBytesAll - doneGlobal);
                                            long etaGs = (long) Math.ceil(leftG / bps);
                                            etaGlobalStr = fmtDuration(etaGs);
                                        }

                                        String elapsedStr = fmtDuration((long)((now - t0) / 1_000_000_000L));

                                        final double pGlobal = (totalBytesAll > 0)
                                                ? Math.min(1.0, doneGlobal / (double) totalBytesAll) : -1;

                                        final String msgRow = String.format(
                                                "Export läuft… Zeile %d · %.1f MB/s · ETA %s · Dauer %s",
                                                rowIndex, mbps, etaTableStr, elapsedStr);

                                        final String msgGlobal = (pGlobal >= 0)
                                                ? String.format("Gesamt: %.1f%% (%,d / %,d Bytes) · %.1f MB/s · ETA %s",
                                                pGlobal * 100.0, doneGlobal, totalBytesAll, mbps, etaGlobalStr)
                                                : String.format("Gesamt: (%,d Bytes) · %.1f MB/s", doneGlobal, mbps);

                                        Platform.runLater(() -> {
                                            if (row != null) row.setMessage(msgRow);
                                            if (pGlobal >= 0) Progress.setProgress(pGlobal);
                                            InfoLabel.setText(msgGlobal);
                                        });

                                        bytesSinceUi = 0L;
                                        lastUiNs = now;
                                    }

                                    logger.debug("[{}] BLOB row {} col {}: {} bytes in {} ms",
                                            tableName, rowIndex, colName, added, ms(tB1 - tB0));

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
                    }

                    csvWriter.flush();

                    // Restbytes (non-LOB/CSV) pauschal gutschreiben, falls Segmentgröße > 0
                    if (tableBytes > 0 && tableDone < tableBytes) {
                        long missing = tableBytes - tableDone;
                        long doneG = bytesDone.addAndGet(missing);
                        tableDone += missing;
                        if (row != null) {
                            long cur = row.getBytesDone();
                            long inc = missing;
                            Platform.runLater(() -> row.setBytesDone(cur + inc));
                        }
                        if (totalBytesAll > 0) {
                            double p = Math.min(1.0, doneG / (double) totalBytesAll);
                            Platform.runLater(() -> Progress.setProgress(p));
                        }
                    }

                    // CSV in ZIP
                    zos.putNextEntry(new ZipEntry("export.csv"));
                    Files.copy(tmpCsv, zos);
                    zos.closeEntry();

                    // Abschluss
                    long tEnd = System.nanoTime();
                    long elapsedSec = Math.max(1L, (tEnd - t0) / 1_000_000_000L);
                    double avgBps   = (double) tableDone / (double) elapsedSec;
                    double avgMBps  = avgBps / (1024.0 * 1024.0);
                    String elapsedStr = fmtDuration(elapsedSec);
                    final String finalMsg = String.format(
                            "Fertig – Zeilen: %d · Dauer %s · ∅ %.1f MB/s",
                            rowIndex, elapsedStr, avgMBps);

                    if (row != null) {
                        Platform.runLater(() -> {
                            row.setMessage(finalMsg); // detailierte Meldung
                            row.setStatus(elapsedStr); // Status = Laufzeit
                        });
                    }

                } catch (java.util.concurrent.CancellationException cx) {
                    try { Files.deleteIfExists(Path.of(zipPath)); } catch (IOException ignore) {}
                    if (row != null) {
                        Platform.runLater(() -> {
                            row.setStatus("Abgebrochen");
                            row.setMessage("Export abgebrochen");
                        });
                    }
                    if (!isCancelled()) cancel(true);
                    throw cx;

                } catch (Exception ex) {
                    if (row != null) {
                        final String msg = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                        Platform.runLater(() -> { row.setStatus("Fehler"); row.setMessage(msg); });
                    }
                    throw ex;

                } finally {
                    runningStmts.remove(tableName);
                    try { Files.deleteIfExists(tmpCsv); } catch (IOException ignore) {}
                }

                return null;
            }
        };
    }

    // ---------------------------------------------------
    // Cancel pro Zeile
    // ---------------------------------------------------
    private void cancelJob(String table) {
        Task<Void> t = runningTasks.get(table);
        if (t != null && t.isRunning()) t.cancel(true);
        Statement s = runningStmts.get(table);
        if (s != null) { try { s.cancel(); } catch (SQLException ignore) {} }
        ExportItem ei = itemsByTable.get(table);
        if (ei != null) {
            ei.setStatus("Abgebrochen");
            ei.setMessage("Durch Benutzer gestoppt");
        }
    }

    // ---------------------------------------------------
    // DB-/Helper-Methoden
    // ---------------------------------------------------
    private Connection newConnection() throws SQLException {
        return DriverManager.getConnection(DbConfig.getUrl(), DbConfig.getUser(), DbConfig.getPassword());
    }

    private int safeGetCountRows(String table) {
        String sql = "SELECT COUNT(*) FROM " + table;
        try (Connection c = newConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            showError("COUNT(*) für " + table + " fehlgeschlagen: " + e.getMessage());
            return 0;
        }
    }

    // Erweiterte Size-Query (TABLE + PARTITION/SUBPARTITION + NESTED TABLE + LOB* inkl. PARTITION/SUBPARTITION)
    private long getTableSizeBytes(String table) {
        final String sql =
                "SELECT NVL(SUM(bytes),0) AS BYTES " +
                        "FROM user_segments s " +
                        "WHERE (s.segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION'," +
                        "                            'NESTED TABLE','NESTED TABLE PARTITION','NESTED TABLE SUBPARTITION') " +
                        "       AND s.segment_name = ?) " +
                        "   OR (s.segment_type IN ('LOBSEGMENT','LOB PARTITION','LOB SUBPARTITION','LOBINDEX','LOB INDEX PARTITION') " +
                        "       AND s.segment_name IN (SELECT segment_name FROM user_lobs WHERE table_name = ?))";
        try (Connection c = newConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            String t = table.toUpperCase(Locale.ROOT);
            ps.setString(1, t);
            ps.setString(2, t);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            showError("Größe (Bytes) für " + table + " fehlgeschlagen: " + e.getMessage());
            return 0L;
        }
    }

    private List<String> fetchTableNames() {
        List<String> tables = new ArrayList<>();
        try (Connection c = newConnection();
             PreparedStatement ps = c.prepareStatement("SELECT table_name FROM user_tables ORDER BY table_name");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) tables.add(rs.getString(1));
        } catch (SQLException e) {
            showError("Fehler beim Laden der Tabellen: " + e.getMessage());
        }
        return tables;
    }

    // ---- BLOB -> ZIP (Streaming) + Cancel ----
    private static long copyBlobToZipCounted(ZipOutputStream zos, Blob blob, String entryName, Task<?> task)
            throws IOException, SQLException {
        long sum = 0L;
        zos.putNextEntry(new ZipEntry(entryName));
        try (InputStream in = blob.getBinaryStream()) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (task != null && task.isCancelled()) {
                    throw new java.util.concurrent.CancellationException("Cancelled during BLOB streaming");
                }
                zos.write(buffer, 0, read);
                sum += read;
            }
        } finally {
            try { zos.closeEntry(); } catch (IOException ignore) {}
            try { blob.free(); } catch (Throwable ignore) {}
        }
        return sum;
    }

    // ---- Dual-List Utils ----
    private static void moveOne(String item, ObservableList<String> from, ObservableList<String> to) {
        if (from.remove(item)) to.add(item);
    }
    private static void moveSelected(ListView<String> fromList,
                                     ObservableList<String> from,
                                     ObservableList<String> to) {
        List<String> selected = new ArrayList<>(fromList.getSelectionModel().getSelectedItems());
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
    private static void installDrag(final ListView<String> list) {
        list.setOnDragDetected(e -> {
            List<String> selected = new ArrayList<>(list.getSelectionModel().getSelectedItems());
            if (selected.isEmpty()) return;
            javafx.scene.input.Dragboard db = list.startDragAndDrop(TransferMode.MOVE);
            ClipboardContent content = new ClipboardContent();
            content.put(DND_FORMAT, String.join("\n", selected));
            db.setContent(content);
            e.consume();
        });
    }
    private static void installDrop(final ListView<String> targetList,
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
                List<String> items = Arrays.asList(payload.split("\\R"));
                sourceItems.removeAll(items);
                targetItems.addAll(items);
                sortIfNeeded(targetItems);
                success = true;
            }
            e.setDropCompleted(success);
            e.consume();
        });
    }

    // ---- UI Helper ----
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

    // ---- Zeit-/Format-Helfer ----
    private static long ms(long nanos) { return nanos / 1_000_000L; }
    private static String fmtDuration(long seconds) {
        long h = seconds / 3600;
        long m = (seconds % 3600) / 60;
        long s = seconds % 60;
        if (h > 0) return String.format("%d:%02d:%02d", h, m, s);
        return String.format("%02d:%02d", m, s);
    }

    // ---------------------------------------------------
    // Model für TableView
    // ---------------------------------------------------
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
                progress.set(Math.max(0.0, Math.min(1.0, p)));
            };
            bytesDone.addListener(recalc);
            bytesTotal.addListener(recalc);
        }

        public String getTable() { return table.get(); }
        public void   setTable(String v) { table.set(v); }
        public StringProperty tableProperty() { return table; }

        public String getSizeGb() { return sizeGb.get(); }
        public void setSizeGb(String v) { sizeGb.set(v); }
        public StringProperty sizeGbProperty() { return sizeGb; }

        public int getRows() { return rows.get(); }
        public void setRows(int v) { rows.set(v); }
        public IntegerProperty rowsProperty() { return rows; }

        public long getBytesDone() { return bytesDone.get(); }
        public void setBytesDone(long v) { bytesDone.set(v); }
        public LongProperty bytesDoneProperty() { return bytesDone; }

        public long getBytesTotal() { return bytesTotal.get(); }
        public void setBytesTotal(long v) { bytesTotal.set(v); }
        public LongProperty bytesTotalProperty() { return bytesTotal; }

        public String getStatus() { return status.get(); }
        public void setStatus(String v) { status.set(v); }
        public StringProperty statusProperty() { return status; }

        public String getMessage() { return message.get(); }
        public void setMessage(String v) { message.set(v); }
        public StringProperty messageProperty() { return message; }

        public ReadOnlyDoubleProperty progressProperty() { return progress.getReadOnlyProperty(); }
    }
}
