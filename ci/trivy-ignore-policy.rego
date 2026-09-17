# Trivy ignore policy for the scan:trivy:vuln job (ci/security-scan.yml).
#
# Why a Rego policy and not .trivyignore: the ignore files match on an explicit
# finding ID (a CVE), so they cannot express "ignore every finding of a package".
# Package-level exclusions have to go through --ignore-policy.
#
# Syntax note: Trivy evaluates this with Rego *v0* (pkg/result/filter.go pins
# ast.RegoV0), so rules are written WITHOUT the `if` keyword and without
# `import rego.v1` - both would fail to compile.
#
# The policy is applied at scan time, so excluded findings never reach
# trivy-vuln.json and therefore not the GitLab report, the JUnit report, the
# HTML report or the verify:security gate. Trivy still records them under
# ExperimentalModifiedFindings in the JSON, so the exclusion stays auditable.
#
# Maven packages are named "groupId:artifactId" (e.g. io.netty:netty-common),
# hence the prefix match below.

package trivy

default ignore = false

# io.netty:* - Netty is provided and version-managed by Elasticsearch itself.
# Its CVEs are resolved by moving to the ES version that fixes them, not by
# anything in this repository. See also ci/owasp-suppressions.xml.
ignore {
	startswith(input.PkgName, "io.netty:")
}
