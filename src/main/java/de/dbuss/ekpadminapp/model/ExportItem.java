package de.dbuss.ekpadminapp.model;

import javafx.beans.property.*;
import javafx.beans.value.ChangeListener;

public class ExportItem {
    private final StringProperty table = new SimpleStringProperty();
    private final StringProperty sizeGb = new SimpleStringProperty("0.00");
    private final IntegerProperty rows = new SimpleIntegerProperty(0);

    private final LongProperty bytesDone = new SimpleLongProperty(0);
    private final LongProperty bytesTotal = new SimpleLongProperty(0);

    private final StringProperty status = new SimpleStringProperty("Geplant");
    private final StringProperty message = new SimpleStringProperty("");

    // progress = bytesDone / bytesTotal
    private final ReadOnlyDoubleWrapper progress = new ReadOnlyDoubleWrapper(0);

    public ExportItem(String table) {
        this.table.set(table);
        // progress updaten, wenn done/total sich ändern
        ChangeListener<Number> recalc = (obs, o, n) -> {
            long done = bytesDone.get();
            long total = bytesTotal.get();
            progress.set((total > 0) ? Math.min(1.0, (double) done / total) : 0.0);
        };
        bytesDone.addListener(recalc);
        bytesTotal.addListener(recalc);
    }

    // Getter/Setter/Properties (nur das Nötigste)
    public String getTable() { return table.get(); }
    public StringProperty tableProperty() { return table; }

    public StringProperty sizeGbProperty() { return sizeGb; }
    public IntegerProperty rowsProperty() { return rows; }

    public LongProperty bytesDoneProperty() { return bytesDone; }
    public LongProperty bytesTotalProperty() { return bytesTotal; }

    public StringProperty statusProperty() { return status; }
    public StringProperty messageProperty() { return message; }

    public ReadOnlyDoubleProperty progressProperty() { return progress.getReadOnlyProperty(); }
}
