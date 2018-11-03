#!groovy

node() {

    def projectName
    def version
    def containerRegistry
    def imageName
    def detailedName
    def latestName

    def shortCommit

    def releaseStage

    stage('Checkout') {
        def scmVars = checkout scm
        shortCommit = scmVars.GIT_COMMIT.take(7)

        projectName = appInfo('name')
        version = appInfo('version')
        containerRegistry = appInfo('registry')
        imageName = appInfo('image')
        println imageName
        
        detailedName = "$imageName-$shortCommit"
        println imageName
        latestName = "$containerRegistry/$projectName:latest"
        println latestName
    }

    stage('Test') {
        if (env.BRANCH_NAME != 'master') {
            println env.BRANCH_NAME
            return
        }
        def testImage = "$projectName-test:$version"
        sh "docker build -t $testImage ."
        sh "docker run --rm $testImage"
        sh "docker rmi $testImage"
    }

    stage('Package') {
        // Builds and tags docker images
        sh "docker build -t $imageName -t $detailedName -t $latestName ."
        sh "docker push $imageName"
        sh "docker push $detailedName"
        sh "docker push $latestName"

        // Deletes images to free up space
        sh "docker rmi $latestName"
        sh "docker rmi $imageName"
        sh "docker rmi $detailedName"
    }

    stage('Deploy') {
        echo "Run deployment to kubernetes here"
    }

}


def appInfo(String command) {
    return sh(script: "appv $command", returnStdout: true).trim()
}