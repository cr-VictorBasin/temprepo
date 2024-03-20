@Library('jenkins-shared-library@irelease_master') _

properties([
	parameters([
		validatingString(name: 'username', defaultValue: '', description: '', failedValidationMessage: 'not a valid username', regex: '^[a-z.-]{3,}$'),
		string(name: 'targetBranch', defaultValue: '', description: '', trim: true),
		choice(name: 'product', choices: ['all-product', 'sensor', 'server', 'data-infra'], description: '''Do not create products that you don\'t need. You will affect resources and signature Branches / Tags will be created in all repositories anyways.'''),
		string(name: 'squad', defaultValue: 'generic', description: '', trim: true),
		booleanParam(name: 'server_using_lumina', defaultValue: true, description: 'create pipeline that support server lumina architecture.'),
		booleanParam(name: 'create_msi_debug', defaultValue: false, description: ''),
		booleanParam(name: 'linux_debug', defaultValue: false, description: 'Trigger Linux Debug build'),
		booleanParam(name: 'add_to_cyberops_db', defaultValue: true, description: ''),
		booleanParam(name: 'slack_channel', defaultValue: true, description: 'create slack channel')
	]),
	buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '2', daysToKeepStr: '', numToKeepStr: '100')),
	throttleJobProperty(categories: [], limitOneJobWithMatchingParams: false, maxConcurrentPerNode: 0, maxConcurrentTotal: 50, paramsToUseForLimit: '', throttleEnabled: true, throttleOption: 'project')
])

Map<String, String> jobParams = new HashMap<String, String>()
// python code parameters
jobParams.put("username", params.username)
jobParams.put("targetBranch", params.targetBranch)
jobParams.put("product", params.product)
jobParams.put("add_to_cyberops_db", params.add_to_cyberops_db)
jobParams.put("create_msi_debug", params.create_msi_debug)
// Linux Debug parameter
jobParams.put("linux_debug", params.linux_debug)
jobParams.put("server_using_lumina", params.server_using_lumina)
jobParams.put("squad", params.squad)
jobParams.put("jobName", env.JOB_NAME)
jobParams.put("buildNumber", env.BUILD_NUMBER)
jobParams.put("creation_type", "create_jobs")
// groovy code parameters
jobParams.put("triggerGetBranches", true)
jobParams.put("actionMethod", "create_jobs")
jobParams.put("slack_channel", params.slack_channel)

// call to JSL function
if (targetBranch.contains("release-19.") || targetBranch.contains("rc-19.") || targetBranch.contains("release-20.1")) {
	println("Executing old pipelines creation")
	re_management_pipelines_old(jobParams)
}
else {
	println("Executing New pipelines creation")
	re_management_pipelines(jobParams)
}
