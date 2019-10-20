/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fifaworldcup;

import javafx.application.Application;
import javafx.stage.Stage;

/**
 *
 * @author Utente
 */
public class FIFAWorldCup extends Application {
    
    @Override
    public void start(Stage primaryStage) {
        
        HomeFrame homeFrame = new HomeFrame();
        homeFrame.setVisible(true);
        
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        launch(args);
    }
    
}
