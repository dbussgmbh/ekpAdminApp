module de.dbuss.ekpadminapp {
    requires javafx.controls;
    requires javafx.fxml;


    opens de.dbuss.ekpadminapp to javafx.fxml;
    exports de.dbuss.ekpadminapp;
}