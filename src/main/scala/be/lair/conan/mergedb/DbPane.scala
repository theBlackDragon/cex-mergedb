package be.lair.conan.mergedb

import javafx.scene.control.{Button, TextField}
import javafx.scene.layout.{GridPane, Pane}
import javafx.scene.text.Text

object DbPane {
  def create(): Pane = {
    val pane = new GridPane
    pane.maxWidth(Double.MaxValue)

    val dbField = new TextField()
    val dbButton = new Button("Select...")

    pane.add(new Text("Save DB 2:"), 0, 0)
    pane.add(dbField, 1, 0)
    pane.add(dbButton, 2, 0)

    pane
  }
}
