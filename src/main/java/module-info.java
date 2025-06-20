module de.dbuss.ekpadminapp {
    requires javafx.controls;
    requires javafx.fxml;


    opens de.dbuss.ekpadminapp to javafx.fxml;
    exports de.dbuss.ekpadminapp;
    exports de.dbuss.ekpadminapp.Controller;
    opens de.dbuss.ekpadminapp.Controller to javafx.fxml;
}