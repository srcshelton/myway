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
		sh 'mkdir -p src'
		dir( 'src' ) {
			gitClean()
			checkout scm
		}

		stage 'Build'
		try {
			sh '''
			set +e

			http_proxy="${HTTP_PROXY:-}"
			https_proxy="${HTTPS_PROXY:-${HTTP_PROXY:-}}"
			export http_proxy https_proxy

			#cpan="/usr/bin/cpanm"
			cpan="/usr/bin/cpan"

			for shsrc in *.sh; do
				[ -r "${shsrc}" ] && if ! bash -n "${shsrc}" >/dev/null 2>&1; then
					echo >&2 "bash syntax check on '${shsrc}' failed:"
					bash -n "${shsrc}" >&2
					exit 1
				fi
			done

			for plsrc in *.pl; do
				if [ -r "${plsrc}" ]; then
					if ! [ -e ~/perl/lib/perl5/local/lib.pm ]; then
						llver="2.000019"
						wget "http://search.cpan.org/CPAN/authors/id/H/HA/HAARG/local-lib-${llver}.tar.gz"
						tar -xzpf "local-lib-${llver}.tar.gz"
						cd "local-lib-${llver}"
						perl Makefile.PL --bootstrap=~/perl --no-manpages
						make test
						make install
						cd -
					fi
					eval "$(perl -I${HOME}/perl/lib/perl5 -Mlocal::lib)"

					case "$( basename "${cpan}" )" in
						cpan)
							{
								echo y
								echo o conf prerequisites_policy follow
								echo o conf make_install_arg UNINST=0
								echo o conf makepl_arg "PREFIX=~/perl LIB=~/perl/lib/perl5 INSTALLSITEMAN1DIR=~/perl/man/man1 INSTALLSITEMAN3DIR=~/perl/man/man3 INSTALLSCRIPT=~/perl/bin INSTALLBIN=~/perl/bin"
								echo o conf mbuildpl_arg "--install_base ~/perl --lib=~/perl/lib/perl5 --installman1dir=~/perl/man/man1 --installman3dir=~/perl/man/man3 --installscript=~/perl/bin --installbin=~/perl/bin"
								echo o conf commit
							} | "${cpan}" --version || true
							;;
						*)
							:
							;;
					esac

					MANPATH="${MANPATH:+${MANPATH}:}~/perl/man"
					export MANPATH

					grep -w 'use' "${plsrc}" |
						sed 's/^\\s*// ; s/#.*$//' |
						grep '^use\\s' |
						tr -s [:space:] |
						cut -d ' ' -f 2 |
						sed 's/;\\s*$//' |
						sort |
						uniq |
						while read M
					do
						# Unbelievable... :(
						if [ "${M}" = "match::smart" ]; then
							M="match::simple"
						fi

						perl -M"${M}" -e 'print( "ok\\n" );' || {
							echo >&2 "Invoking '${cpan}' to install Perl module '${M}' as run-time dependency of '${plsrc}' ..."
							"${cpan}" -i "${M}"
						}
					done

					if ! perl -c "${plsrc}" >/dev/null 2>&1; then
						echo >&2 "perl syntax check on '${plsrc}' failed:"
						perl -c "${plsrc}" >&2
						exit 1
					fi
				fi
			done

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

/**
 * Clean a Git project workspace.
 * Uses 'git clean' if there is a repository found.
 * Uses Pipeline 'deleteDir()' function if no .git directory is found.
 */
def gitClean() {
	timeout(time: 60, unit: 'SECONDS') {
		if( fileExists( '.git' ) ) {
			echo 'Found git repository: using git to clean the tree.'
			// The sequence of reset --hard and clean -fdx first
			// in the root and then using submodule foreach
			// is based on how the Jenkins Git SCM clean before checkout
			// feature works.
			sh 'git reset --hard'
			// Note: -e is necessary to exclude the temp directory
			// .jenkins-XXXXX in the workspace where Pipeline puts the
			// batch file for the 'bat' command.
			sh 'git clean -ffdx -e ".jenkins-*/"'
			sh 'git submodule foreach --recursive git reset --hard'
			sh 'git submodule foreach --recursive git clean -ffdx'
		} else {
			echo 'No git repository found: using deleteDir() to clean workspace'
			deleteDir()
		}
	}
}

// vi: set syntax=groovy:
