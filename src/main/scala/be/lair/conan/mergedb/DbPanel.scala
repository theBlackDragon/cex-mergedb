package be.lair.conan.mergedb

import java.awt.Color
import java.awt.event.{ActionEvent, FocusEvent, FocusListener}
import java.io.File

import javax.swing.{BoxLayout, JButton, JComponent, JFileChooser, JPanel, JTextField}

class DbPanel(parent: Option[JComponent] = None) extends JPanel {

  private var selection: Option[File] = None

  setLayout(new BoxLayout(this, BoxLayout.X_AXIS))
  private val outputDbTextField = new JTextField(20)
  outputDbTextField.addFocusListener(new FocusListener {
    override def focusGained(focusEvent: FocusEvent): Unit = {}

    override def focusLost(focusEvent: FocusEvent): Unit = {
      val selectedFile = new File(outputDbTextField.getText())
      if(selectedFile.exists()) {
        outputDbTextField.setBackground(Color.WHITE)
        selection = Option(selectedFile)
      } else {
        if(!outputDbTextField.getText.isBlank)
          outputDbTextField.setBackground(Color.RED)
      }
    }
  })

  private val outputDbButton = new JButton("Select...")

  outputDbButton.addActionListener { (_: ActionEvent) =>
    val chooser = new JFileChooser()
    chooser.showOpenDialog(this) match {
      case JFileChooser.APPROVE_OPTION =>
        val selectedFile = chooser.getSelectedFile
        outputDbTextField.setText(selectedFile.getAbsolutePath)
        selection = Some(selectedFile)
    }

  }

  add(outputDbTextField)
  add(outputDbButton)

  parent match {
    case Some(parent) =>
      val removeButton = new JButton("-")
      add(removeButton)
      removeButton.addActionListener { (_: ActionEvent) =>
        parent.remove(this)
      }
    case None =>
  }

  def selectedFile: Option[File] = selection
}
