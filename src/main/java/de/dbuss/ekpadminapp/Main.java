package de.dbuss.ekpadminapp;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class Main extends Application {

    private static Stage primaryStage;

    @Override
    public void start(Stage stage) throws Exception {

        primaryStage = stage;

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/de/dbuss/ekpadminapp/view/Login.fxml"));
        Scene scene = new Scene(loader.load(),650,450);
        primaryStage.setScene(scene);
        primaryStage.setTitle("eKP Admin App Login");
        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }


    public static Stage getPrimaryStage() {
        return primaryStage;
    }

}