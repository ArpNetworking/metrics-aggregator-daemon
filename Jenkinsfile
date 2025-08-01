pipeline {
  agent {
    kubernetes {
      defaultContainer 'ubuntu'
      activeDeadlineSeconds 3600
    }
  }
  options {
    ansiColor('xterm')
  }
  stages {
    stage('Init') {
      steps {
        checkout scm
	script {
	  def m = (env.GIT_URL =~ /(\/|:)(([^\/]+)\/)?(([^\/]+?)(\.git)?)$/)
	  if (m) {
	    org = m.group(3)
	    repo = m.group(5)
	  }
	}
        discoverReferenceBuild()
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
            usernamePassword(credentialsId: 'jenkins-central', usernameVariable: 'CENTRAL_USER', passwordVariable: 'CENTRAL_PASS'),
            string(credentialsId: 'jenkins-gpg', variable: 'GPG_PASS')]) {
          sh '''
          echo "=== Docker Environment Debug ==="
          echo "DOCKER_HOST=$DOCKER_HOST"
          echo "DOCKER_TLS_VERIFY=$DOCKER_TLS_VERIFY"
          echo "DOCKER_CERT_PATH=$DOCKER_CERT_PATH"
          
          echo "=== Creating Docker Context ==="
          docker context create multiarch-context --docker "host=$DOCKER_HOST,ca=/certs/client/ca.pem,cert=/certs/client/cert.pem,key=/certs/client/key.pem" || echo "Context may already exist"
          
          echo "=== Listing Docker Contexts ==="
          docker context ls
          
          echo "=== Creating Buildx Builder ==="
          docker buildx create --name multiarch --driver docker-container --use multiarch-context || docker buildx use multiarch
          
          echo "=== Listing Buildx Builders ==="
          docker buildx ls
          '''
          withMaven {
            sh """
            export DOCKER_TLS_VERIFY=1
            echo "Using existing DOCKER_HOST=\$DOCKER_HOST with TLS, DOCKER_CERT_PATH=\$DOCKER_CERT_PATH"
            ./jdk-wrapper.sh ./mvnw $target -P rpm -U -B -Dstyle.color=always -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Ddocker.verbose=true
            """
          }
        }
      }
    }
    stage('GitHub release') {
      when { buildingTag(); not { changeRequest() }  }
      steps {
        withCredentials([usernamePassword(credentialsId: 'brandonarp-github-token', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
          sh "github-release release --user ${org} --repo ${repo} --tag ${TAG_NAME}"
          sh "sleep 5"
          sh "github-release upload --user ${org} --repo ${repo} --tag ${TAG_NAME} --name ${TAG_NAME}.tgz --file target/*.tgz"
          sh "github-release upload --user ${org} --repo ${repo} --tag ${TAG_NAME} --name ${TAG_NAME}.rpm --file target/rpm/*/RPMS/*/*.rpm"
        }
      }
    }
    stage('Save cache') {
      when {
        branch 'master'
      }
      steps {
        withMaven {
          sh "./jdk-wrapper.sh ./mvnw -Ddocker.image.tag=cache-base -Ddocker.push.registry=docker.arpnetworking.com docker:push"
        }
      }
    }
  }
  post('Analysis') {
    always {
      recordCoverage(tools: [[parser: 'JACOCO']])
      recordIssues(
          enabledForFailure: true, aggregatingResults: true,
          tools: [java(), mavenConsole(), javaDoc(), checkStyle(reportEncoding: 'UTF-8'), spotBugs()])
    }
  }
}
