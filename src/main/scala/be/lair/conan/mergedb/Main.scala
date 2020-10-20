package be.lair.conan.mergedb

import java.awt.event._
import java.awt.{BorderLayout, Color}
import java.io.File
import java.nio.file.Files

import be.lair.conan.mergedb.db.Merge
import grizzled.slf4j.Logging
import javax.swing._

object Main extends Logging {

  private var outputFile: Option[File] = None

  def main(args: Array[String]): Unit = {
    Main.create()
  }

  def create(): JFrame = {
    val mainFrame = new JFrame()
    mainFrame.setTitle("Conan Exiles MergeDB")
    mainFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    mainFrame.setResizable(false)

    // define UI
    val addButton = new JButton("Add database to merge")

    val centerPanel = new JPanel(new BorderLayout())
    val inputDbPanel = Box.createVerticalBox()
    inputDbPanel.add(Box.createVerticalStrut(1))
    inputDbPanel.add(new DbPanel())
    inputDbPanel.add(new DbPanel())
    inputDbPanel.addContainerListener(new ContainerAdapter() {
      override def componentAdded(event: ContainerEvent): Unit = {
        mainFrame.pack()
      }

      override def componentRemoved(event: ContainerEvent): Unit = {
        mainFrame.pack()
      }
    })
    centerPanel.add(inputDbPanel)

    val bottomPanel = new JPanel(new BorderLayout())
    val outputDbPanel = Box.createHorizontalBox()
    val outputDbTextField = new JTextField(20)
    val outputDbButton = new JButton("Select...")
    outputDbPanel.add(new JLabel("Output"))
    outputDbPanel.add(outputDbTextField)
    outputDbPanel.add(outputDbButton)

    val mergeButton = new JButton("Merge")

    bottomPanel.add(outputDbPanel, BorderLayout.CENTER)
    bottomPanel.add(mergeButton, BorderLayout.SOUTH)

    /* **********************
     *      Listeners       *
     ********************** */
    addButton.addActionListener { _: ActionEvent =>
      inputDbPanel.add(new DbPanel(Some(inputDbPanel)))
    }

    outputDbButton.addActionListener { _: ActionEvent => {
      val chooser = new JFileChooser()
      chooser.showOpenDialog(mainFrame) match {
        case JFileChooser.APPROVE_OPTION =>
          val selectedFile = chooser.getSelectedFile
          outputDbTextField.setText(selectedFile.getAbsolutePath)
          outputFile = Some(selectedFile)
      }
    }}

    outputDbTextField.addFocusListener(new FocusListener {
      override def focusGained(focusEvent: FocusEvent): Unit = {}

      override def focusLost(focusEvent: FocusEvent): Unit = {
        val selectedFile = new File(outputDbTextField.getText())
        // we don't want a file that exists
        if (!selectedFile.exists()) {
          outputDbTextField.setBackground(Color.WHITE)
          outputFile = Option(selectedFile)
        } else {
          if (!outputDbTextField.getText.isBlank)
            outputDbTextField.setBackground(Color.RED)
        }
      }
    })

    mergeButton.addActionListener { _: ActionEvent => {
      val output = inputDbPanel.getComponents.filter(_.isInstanceOf[DbPanel])
        .flatMap{ case panel: DbPanel => panel.selectedFile}
        .reduceLeft((a, b) => Merge.merge(a, b))

      outputFile match {
        case Some(out) => Files.copy(output.toPath, out.toPath)
        case None => outputDbTextField.setBackground(Color.RED)
      }
    }}

    // add everything to the frame
    mainFrame.add(addButton, BorderLayout.NORTH)
    mainFrame.add(centerPanel, BorderLayout.CENTER)
    mainFrame.add(bottomPanel, BorderLayout.SOUTH)

    mainFrame.pack()
    mainFrame.setVisible(true)

    mainFrame
  }
}
