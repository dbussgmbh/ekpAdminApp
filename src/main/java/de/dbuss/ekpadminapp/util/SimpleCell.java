package de.dbuss.ekpadminapp.util;

import javafx.scene.control.ListCell;

public class SimpleCell extends ListCell<String> {
    @Override protected void updateItem(String item, boolean empty) {
        super.updateItem(item, empty);
        setText(empty ? null : item);
    }
}