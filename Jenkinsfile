pipeline {
  agent {
    kubernetes {
      yamlFile 'kubes-pod.yaml'
      defaultContainer 'ubuntu'
      activeDeadlineSeconds 3600
      idleMinutes 15
    }
  }
  stages {
    stage('Init') {
      steps {
        checkout scm
      }
    }
    stage('Setup build') {
      when { not { buildingTag() } }
      steps {
        script {
          target = "verify"
        }
      }
    }
    stage('Setup release') {
      when { buildingTag(); not { changeRequest() }  }
      steps {
        script {
          target = "deploy -P release -P rpm --settings settings.xml"
        }
        sh 'gpg --batch --import arpnetworking.key'
      }
    }
    stage('Build') {
      steps {
        withCredentials([usernamePassword(credentialsId: 'jenkins-dockerhub', usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD'),
            usernamePassword(credentialsId: 'jenkins-ossrh', usernameVariable: 'OSSRH_USER', passwordVariable: 'OSSRH_PASS'),
            string(credentialsId: 'jenkins-gpg', variable: 'GPG_PASS')]) {
          withMaven {
            sh "./jdk-wrapper.sh ./mvnw $target -P rpm -U -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Ddocker.verbose=true"
          }
        }
      }
    }
    stage('GitHub release') {
      when { buildingTag(); not { changeRequest() }  }
      steps {
        withCredentials([usernamePassword(credentialsId: 'brandonarp-github-token', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
          sh "github-release release --user ArpNetworking --repo metrics-aggregator-daemon --tag ${TAG_NAME}"
          sh "github-release upload --user ArpNetworking --repo metrics-aggregator-daemon --tag ${TAG_NAME} --name metrics-aggregator-daemon-${TAG_NAME}.tgz --file target/metrics-aggregator-daemon*.tgz"
          sh "github-release upload --user ArpNetworking --repo metrics-aggregator-daemon --tag ${TAG_NAME} --name metrics-aggregator-daemon-${TAG_NAME}.rpm --file target/rpm/metrics-aggregator-daemon/RPMS/noarch/metrics-aggregator-daemon-*.rpm"
        }
      }
    }
  }
  post('Analysis') {
    always {
      recordIssues(
          enabledForFailure: true, aggregatingResults: true,
          tools: [java(), checkStyle(reportEncoding: 'UTF-8'), spotBugs()])
    }
  }
}
