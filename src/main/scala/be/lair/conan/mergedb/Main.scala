package be.lair.conan.mergedb

import grizzled.slf4j.Logging
import javafx.application.Application
import javafx.scene.Scene
import javafx.scene.control.{Button, TextField}
import javafx.scene.layout.{GridPane, Pane, VBox}
import javafx.scene.text.Text
import javafx.stage.Stage

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    Application.launch(classOf[Main], args: _*)
  }

  class Main extends Application with Logging {

    override def start(primaryStage: Stage): Unit = {
      // Build GUI
      primaryStage.setTitle("Conan Exiles MergeDB")

      val scene = new Scene(new VBox())

      // --- Main Pane
      val main = createMainPane(primaryStage)
      main.maxWidth(Double.MaxValue)
      main.maxHeight(Double.MaxValue)

      // --- Add to the primary stage
      scene.getRoot.asInstanceOf[VBox].getChildren.add(main)
      primaryStage.setScene(scene)
      primaryStage.show()
    }

    private def createMainPane(stage: Stage): Pane = {
      val pane = new GridPane
      pane.setMaxWidth(Double.MaxValue)
      pane.setMaxHeight(Double.MaxValue)

      val outputFileField = new TextField()
      val mergeButton = new Button("Merge")

      pane.add(DbPane.create(), 0, 0, 2, 1)
      pane.add(DbPane.create(), 0, 1, 2, 1)

      pane.add(new Text("Output DB:"), 0, 2)
      pane.add(outputFileField, 1, 2)

      pane.add(mergeButton, 0, 3)

      pane
    }
  }
}
