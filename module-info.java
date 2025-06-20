module de.dbuss.ekpadminapp {
    requires javafx.controls;
    requires javafx.fxml;

    opens de.dbuss.ekpadminapp.controller to javafx.fxml;
    exports de.dbuss.ekpadminapp;
}