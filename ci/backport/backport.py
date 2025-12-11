import gitlab
import os
import sys

GITLAB_URL = os.environ.get('CI_SERVER_URL')
PRIVATE_TOKEN = os.environ.get('GITLAB_TOKEN')
PROJECT_ID = os.environ.get('CI_PROJECT_ID')
LABEL_PREFIX = "backport-"
BACKPORT_FAILED_LABEL = "backport-failed"
REPO_PATH = os.getcwd()

def get_target_branches(mr_labels):
    """
    Filter labels looking for 'backport-*'
    """
    target_branches = []
    
    for label in mr_labels:
        if label.startswith(LABEL_PREFIX):
            # Odcinamy prefiks 'backport-' i bierzemy resztę jako nazwę gałęzi
            branch_name = label[len(LABEL_PREFIX):]
            target_branches.append(branch_name)
            
    return target_branches

def process_backport(mr_iid):
    try:
        # Authentication
        gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN)
        project = gl.projects.get(PROJECT_ID)
        
        # Get Merge Request
        mr = project.mergerequests.get(mr_iid)
        
        # --- VERYFICATION ---
        target_branches = get_target_branches(mr.labels)
        
        if not target_branches:
            print(f"MR !{mr_iid} dont have backport labels '{LABEL_PREFIX}'. Stoping backport.")
            return

        print(f"MR !{mr_iid} found backport labels. Targets: {target_branches}")
        
        # TUTAJ wstaw całą logikę Git: cherry-pick i tworzenie nowych MR
        # for branch in target_branches:
        #     perform_cherry_pick_and_create_mr(mr, branch)

    except gitlab.exceptions.GitlabError as e:
        print(f"Error in gitlab API communication: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unknown error: {e}")
        sys.exit(1)

def perform_cherry_pick(repo: Repo, mr_commit_hash: str, target_branch: str, new_branch_name: str):
    """Attempts to perform a cherry-pick and creates a new branch."""
    
    print(f"--> Preparing backport for branch: {target_branch}")
    
    # 1. Check if the target branch exists and fetch changes
    try:
        repo.git.fetch('origin', target_branch)
    except GitCommandError:
        print(f"ERROR: Target branch '{target_branch}' does not exist or fetch failed.")
        return False
        
    # 2. Create and checkout the new working branch
    try:
        repo.git.checkout(target_branch, '-b', new_branch_name)
    except GitCommandError as e:
        print(f"ERROR: Could not checkout branch {target_branch}. Details: {e}")
        return False
    
    # 3. Perform cherry-pick
    try:
        print(f"--> Executing cherry-pick of commit: {mr_commit_hash}")
        repo.git.cherry_pick(mr_commit_hash)
        repo.git.push('origin', new_branch_name, force=True)
        print("Cherry-pick successful and pushed.")
        return True
    except GitCommandError as e:
        print(f"Conflict or error during cherry-pick to {target_branch}.")
        print("Resetting local state...")
        # Abort cherry-pick attempt to clean up the working directory
        repo.git.cherry_pick('--abort')
        return False

def scan_repository(mr_iid):
    try:
        # 1. Initialize GitLab client and local repository object
        gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN)
        project = gl.projects.get(PROJECT_ID)
        mr = project.mergerequests.get(mr_iid)
        repo = Repo(REPO_PATH) 
        
        # 2. Validate labels and target branches
        target_branches = get_target_branches(mr.labels)
        mr_commit_hash = mr.sha # The hash of the commit that was merged

        if not target_branches:
            print(f"MR !{mr_iid} does not have '{LABEL_PREFIX}' labels. Exiting.")
            sys.exit(0)
            
        print(f"Found {len(target_branches)} target branches: {target_branches}")

        # 3. Iterate and Backport
        for branch in target_branches:
            new_branch_name = f"backport/mr-{mr_iid}-to-{branch}"
            
            # Step 3a: Perform Git operations
            success = perform_cherry_pick(repo, mr_commit_hash, branch, new_branch_name)
            
            if success:
                # Step 3b: Create a Merge Request in GitLab
                new_mr_title = f"[Backport] Fix: {mr.title} (from !{mr_iid})"
                original_author_id = mr.author['id']
                
                new_mr = project.mergerequests.create({
                    'source_branch': new_branch_name,
                    'target_branch': branch,
                    'title': new_mr_title,
                    'description': f"Automatic backpoort of commit {mr_commit_hash} from Merge Request !{mr_iid}.",
                    'assignee_id': original_author_id,
                    'merge_when_pipeline_succeeds': auto_merge_enabled, # Optional automerge
                    'remove_source_branch': remove_source_branch_enabled # Optional: removes the backport branch upon merge
                })
                print(f"Created new Merge Request: {new_mr.web_url}")

            else:
                # Step 3c: Label the original MR as failed
                print(f"!!! Backport to {branch} failed (Conflict?). Adding failure label.")
                mr.labels = list(set(mr.labels + [BACKPORT_FAILED_LABEL]))
                mr.save()
                
    except GitlabError as e:
        print(f"ERROR: GitLab API Error: {e}")
        sys.exit(1)
    except GitCommandError as e:
        print(f"ERROR: Git Command Error. Could not initialize repository or fetch: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unknown error in main process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backport.py <MR_IID>")
        sys.exit(1)
        
    mr_iid = sys.argv[1]
    process_backport(mr_iid)

    if not PRIVATE_TOKEN or not GITLAB_URL or not PROJECT_ID:
        print("ERROR: Missing required environment variables (GITLAB_PRIVATE_TOKEN, CI_SERVER_URL, CI_PROJECT_ID).")
        sys.exit(1)

    auto_merge_enabled = os.environ.get('AUTO_MERGE_ENABLED', 'True').lower() in ('true', '1', 't')
    remove_source_branch_enabled = os.environ.get('REMOVE_SOURCE_BRANCH_ENABLED', 'True').lower() in ('true', '1', 't')
    
    print(f"Configuration: Auto-Merge: {auto_merge_enabled}, Remove Source Branch: {remove_source_branch_enabled}")

    scan_repository(mr_iid)


