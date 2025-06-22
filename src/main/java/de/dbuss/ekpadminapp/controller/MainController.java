package de.dbuss.ekpadminapp.controller;

import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.BorderPane;

import java.io.IOException;

public class MainController {

    @FXML
    private AnchorPane contentPane;
    @FXML
    private void loadPage1() {
        loadPage("/de/dbuss/ekpadminapp/view/TableView.fxml");
    }

    @FXML
    private void loadPage2() {
        loadPage("/de/dbuss/ekpadminapp/view/page2.fxml");
    }

    private void loadPage(String path) {
        try {
            Node node = FXMLLoader.load(getClass().getResource(path));
            contentPane.getChildren().setAll(node);

            // Optional: Füllung des Ankerbereichs
            AnchorPane.setTopAnchor(node, 0.0);
            AnchorPane.setBottomAnchor(node, 0.0);
            AnchorPane.setLeftAnchor(node, 0.0);
            AnchorPane.setRightAnchor(node, 0.0);



        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handleClose(ActionEvent actionEvent) {
        Platform.exit();
    }
}
