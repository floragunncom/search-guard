import base64
import gitlab
import os
import requests
import sys
import time
from git import Repo, GitCommandError


class GitlabBackport:

    def __init__(self):
        self.gitlab_url = os.environ.get("CI_SERVER_URL")
        self.private_token = os.environ.get("GITLAB_TOKEN")
        self.project_id = os.environ.get("CI_PROJECT_ID", "")
        self.log_level = os.environ.get("BACKPORT_LOG_LEVEL", "info").lower
        self.backport_label_prefix = os.environ.get("BACKPORT_LABEL_PREFIX","backport-")
        self.backport_failed_label = "failed-backport"
        self.repo_path = os.getcwd()
        if not self.private_token:
            print("ERROR: Missing environment variable: GITLAB_TOKEN")
            sys.exit(1)

        if not self.gitlab_url:
            print("ERROR: Missing environment variable: CI_SERVER_URL")
            sys.exit(1)

        if not self.project_id:
            print("ERROR: Missing environment variable: CI_PROJECT_ID")
            sys.exit(1)
        self.auto_merge_enabled = os.environ.get(
            "AUTO_MERGE_ENABLED", "True"
        ).lower() in ("true", "1", "t")
        self.remove_source_branch_enabled = os.environ.get(
            "REMOVE_SOURCE_BRANCH_ENABLED", "True"
        ).lower() in ("true", "1", "t")

        print(
            f"Configuration: Auto-Merge: {self.auto_merge_enabled}, Remove Source Branch: {self.remove_source_branch_enabled}"
        )

        self.gitlab_connection = gitlab.Gitlab(self.gitlab_url, private_token=self.private_token)
        print(f"DEBUG Gitlab connected")
        self.current_project = self.gitlab_connection.projects.get(self.project_id)

        if self.log_level == "debug":
            self.gitlab_connection.enable_debug()
        self.repository = Repo(self.repo_path)
        self.repository.git.config("user.email", "ci-bot@eliatra.com")
        self.repository.git.config("user.name", "GitLab CI Bot")
        # self.repository.remotes.origin.set_url(self.repository.remotes.origin.url.replace("https://oauth2:.*@", "https://")) # extra cleaning
        original_url = self.repository.remotes.origin.url
        print(f"DEBUG Original url {original_url}")
        auth_string = f"oauth2:{self.private_token}"
        auth_base64 = base64.b64encode(auth_string.encode("ascii")).decode("ascii")
        with self.repository.config_writer() as cw:
            cw.set_value("http", "extraHeader", f"Authorization: Basic {auth_base64}")

    def get_mr_iid(self):
        commit_sha = os.environ.get("CI_COMMIT_SHA")

        if not commit_sha:
            print("ERROR: Cannot find CI_COMMIT_SHA")
            sys.exit(1)

        # Endpoint: /projects/:id/repository/commits/:sha/merge_requests
        url = f"{self.gitlab_url}/api/v4/projects/{self.project_id}/repository/commits/{commit_sha}/merge_requests"
        headers = {"PRIVATE-TOKEN": self.private_token}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            mrs = response.json()

            if not mrs:
                print(f"Cannot find Merge Request for commit {commit_sha}")
                sys.exit(0)

            return mrs[0]["iid"]

        except Exception as e:
            print(f"Error using GitLab API: {e}")
            sys.exit(1)

    def get_target_branches(self, mr_labels):
        """
        Filter labels looking for 'backport-*'
        """
        target_branches = []

        for label in mr_labels:
            if label.startswith(self.backport_label_prefix):
                # Remove prefix 'backport-' and take rest as branch name
                branch_name = label[len(self.backport_label_prefix) :]
                target_branches.append(branch_name)

        return target_branches

    def create_backport_mr(
        self,
        source_branch,
        target_branch,
        title,
        description,
        author_id=None,
        has_conflicts=False,
    ):
        """
        Creates a Merge Request for a backport
        """

        # Local copy of auto_merge setting to allow overriding in case of conflicts
        should_enable_mwps = self.auto_merge_enabled

        if has_conflicts:
            title = f"[CONFLICT] {title}"
            description += (
                "\n\nWARNING: Conflicts detected during cherry-pick.\n"
                "This Merge Request was created automatically but contains conflicts. "
                "Please resolve them manually before merging."
            )
            # Cannot auto-merge if conflicts are present
            should_enable_mwps = False

        # 1. Build the MR data payload
        mr_data = {
            "source_branch": source_branch,
            "target_branch": target_branch,
            "title": title,
            "description": description,
            "squash": True,
            "remove_source_branch": self.remove_source_branch_enabled,
        }

        # Only include assignee_id if author_id is provided
        if author_id:
            mr_data["assignee_id"] = author_id

        try:
            new_mr = self.current_project.mergerequests.create(mr_data)  # type: ignore
            print(f"Created new Merge Request: {new_mr.web_url}")
        except Exception as e:
            print(f"Failed to create Merge Request: {e}")
            return None

        # 2. Merge When Pipeline Succeeds (MWPS) Logic
        if should_enable_mwps:
            print("Attempting to enable Merge When Pipeline Succeeds (MWPS)...")
            try:
                for i in range(10):
                    mr = self.current_project.mergerequests.get(new_mr.iid)  # type: ignore

                    if mr.merge_status == "can_be_merged":
                        mr.merge(
                            merge_when_pipeline_succeeds=True,
                            should_remove_source_branch=self.remove_source_branch_enabled,
                        )
                        print(f"MWPS enabled successfully after {i*2} seconds.")
                        break

                    time.sleep(2)
            except Exception as e:
                print(f"Could not enable MWPS: {e}")

        return new_mr

    def perform_cherry_pick(
        self,
        mr_commit_hash: str,
        target_branch: str,
        new_branch_name: str,
        author=str | None,
    ):
        """Attempts to perform a cherry-pick and creates a new branch."""

        print(f"--> Preparing backport for branch: {target_branch}")
        # 1. Check if the target branch exists and fetch changes
        try:
            self.repository.git.fetch("origin", target_branch)
        except GitCommandError as e:
            print(
                f"ERROR: Target branch '{target_branch}' does not exist or fetch failed."
            )
            print("-" * 30)
            print(f"Git Exit Code: {e.status}")
            print("Git Stderr Output:")
            print(e.stderr)
            return False

        # 2. Create and checkout the new working branch
        try:
            self.repository.git.checkout(f"origin/{target_branch}", "-b", new_branch_name)
        except GitCommandError as e:
            print(f"ERROR: Could not checkout branch {target_branch}. Details: {e}")
            return False

        # 3. Perform cherry-pick
        try:
            print(f"--> Executing cherry-pick of commit: {mr_commit_hash}")
            self.repository.git.cherry_pick(mr_commit_hash)
            self.repository.git.push("origin", new_branch_name, force=False)
            print("Cherry-pick successful and pushed.")
            return True
        except GitCommandError as e:
            print(f"Conflict or error during cherry-pick to {target_branch}.")
            print("-" * 30)
            print(f"Git Exit Code: {e.status}")
            print("Git Stderr Output:")
            print(e.stderr)

            # Identify specific files causing the conflict
            try:
                self.repository.git.add(A=True)
                self.repository.git.commit(
                    "-m",
                    f"Backport conflicts in {mr_commit_hash} - manual fix required",
                    "--no-verify",
                )
                self.repository.git.push("origin", new_branch_name, force=False)
                title = f"Confilct found in backport {new_branch_name}"
                description = (
                    "Warrnig conficts found!\n\n" "Manual correction is needed"
                )
                unmerged_files = self.repository.git.diff("--name-only", "--diff-filter=U")
                if unmerged_files:
                    print(f"Conflicting files identified:\n{unmerged_files}")
                    description += f"Conflicting files identified:\n{unmerged_files}"
                self.create_backport_mr(
                    new_branch_name, target_branch, title, description, author
                )
                print(
                    f"!!! Branch {new_branch_name} pushed with conflicts. User can now fix it manually."
                )

            except Exception:
                # Fallback if diff fails during conflict state
                pass
            print("Resetting local state...")
            # Abort cherry-pick attempt to clean up the working directory
            self.repository.git.cherry_pick("--abort")
            return False

    def scan_repository(self):
        mr_iid = self.get_mr_iid()
        print("DEBUG Scan repository")
        try:
            mr = self.current_project.mergerequests.get(mr_iid)
            # 2. Validate labels and target branches
            target_branches = self.get_target_branches(mr.labels)
            mr_commit_hash = mr.sha  # The hash of the commit that was merged
            if not target_branches:
                print(
                    f"MR !{mr_iid} does not have '{self.backport_label_prefix}' labels. Exiting."
                )
                sys.exit(0)
            print(f"Found {len(target_branches)} target branches: {target_branches}")
            # 3. Iterate and Backport
            for branch in target_branches:
                new_branch_name = f"backport/mr-{mr_iid}-to-{branch}"
                original_author_id = mr.author["id"]
                # Step 3a: Perform Git operations
                success = self.perform_cherry_pick(
                    mr_commit_hash, branch, new_branch_name, original_author_id
                )
                if success:
                    # Step 3b: Create a Merge Request in GitLab
                    new_mr_title = f"[Backport] Fix: {mr.title} (from !{mr_iid})"
                    description = f"Automatic backport of commit {mr_commit_hash} from Merge Request !{mr_iid}."
                    self.create_backport_mr(
                        new_branch_name,
                        branch,
                        new_mr_title,
                        description,
                        original_author_id,
                    )
                else:
                    # Step 3c: Label the original MR as failed
                    print(
                        f"!!! Backport to {branch} failed (Conflict?). Adding failure label."
                    )
                    mr.labels = list(set(mr.labels + [self.backport_failed_label]))
                    mr.save()

        except gitlab.GitlabError as e:
            print(f"ERROR: GitLab API Error: {e}")
            sys.exit(1)
        except GitCommandError as e:
            print(
                f"ERROR: Git Command Error. Could not initialize repository or fetch: {e}"
            )
            sys.exit(1)
        except Exception as e:
            print(f"Unknown error in main process: {e}")
            sys.exit(1)


if __name__ == "__main__":

    backport = GitlabBackport()
    backport.scan_repository()
