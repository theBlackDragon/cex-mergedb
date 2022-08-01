pipeline {
  agent any

  tools {
    jdk 'JDK11'
  }

  options {
    timestamps()
  }

  environment {
    SBT_HOME = tool 'Sbt 1.1'
  }

  triggers {
    pollSCM('H/4 * * * *')
  }

  stages {
    stage('Build') {
      steps {
        sh '$SBT_HOME/bin/sbt -no-colors clean package'
      }
      post {
        success {
          archiveArtifacts(artifacts: '**/target/**/*.jar', allowEmptyArchive: true)
        }
      }
    }
  }
}
