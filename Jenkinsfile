withEnv( [
	  'JENKINS_SFTP_SERVERS=/var/lib/jenkins/iod/software_servers.cfg'
	, 'JENKINS_SERVER_TAGS=all'
	, 'JENKINS_BUILD_TAG=master'
	, 'JENKINS_COMPONENT_CFG=.build/package.cfg'
	, 'JENKINS_HISTORY_DIR=/var/lib/jenkins/iod/history'
] ) {
	def nodeTag = getNode()

	node( nodeTag ) {
		env.WORKSPACE = pwd()
		env.JENKINS_BUILD_TAG = getBranchTag()
		env.JENKINS_SERVER_TAGS = getSCPTags()

		stage 'Checkout'
		checkout scm

		stage 'Build'
		try {
			sh '''
			export http_proxy="${HTTP_PROXY:-}"
			export https_proxy="${HTTPS_PROXY:-${HTTP_PROXY:-}}"

			/var/lib/jenkins/iod/build-scripts/build-components.sh
			'''

			currentBuild.result = 'SUCCESS'
		} catch( Exception err ) {
			currentBuild.result = 'FAILURE'
		}

		stage 'Notifications'
		echo "Please ignore 'Job type does not allow token replacement.' warnings below, which come from the email plugin..."
		emailext \
			  to:		'$DEFAULT_RECIPIENTS' \
			, subject:	"JENKINS - ${env.JOB_NAME} - Build #${env.BUILD_NUMBER}: ${currentBuild.result}" \
			, body:		"View console output at ${env.BUILD_URL}\n\n" \
			, attachLog:	 true
	}
}

def getNode() {
	def matcher = env.BRANCH_NAME =~ '(release|develop|stable|hotfix)'
	matcher ? 'persistent-builds' : 'feature-builds'
}

def getBranchTag() {
	def matcher = env.BRANCH_NAME =~ '(release|feature|hotfix)/(.*)'
	matcher ? matcher[0][1] + '-' + matcher[0][2] : env.BRANCH_NAME
}

def getSCPTags() {
	def matcher = env.BRANCH_NAME =~ 'feature'
	matcher ? 'ops-tools-dev' : 'all'
}
