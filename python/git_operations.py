import os
import re
from typing import Any, Dict, List, Pattern

from git import GitCommandError, Repo
from loguru import logger


# Does not delete the repo folder if there is any error
def clone_repository(
    repo: str,
    context: Dict[str, Any],
    user: str,
    access_token: str,
    repo_location: str = "./repo",
) -> tuple[bool, str]:
    """ """

    try:
        clone_url = f"https://{user}:{access_token}@git.worten.net/{repo}"
        download_repo = Repo.clone_from(clone_url, repo_location)
        most_recent_commit = str(download_repo.head.commit)
        return True, most_recent_commit
    except GitCommandError as e:
        logger.error(
            "[GIT] Error cloning repository",
            fields={"context": context, "error_message": str(e)},
        )
        return False, ""


def retrieve_commit_changes(path: str, branch: str | None) -> str:
    try:
        repo = Repo(path)
        if repo is not None:
            root_dir = repo.git.rev_parse("--show-toplevel")
            if os.getcwd() != root_dir:
                os.chdir(root_dir)

            if branch is not None:
                current_branch = branch
            else:
                current_branch = repo.active_branch.name

            logger.info(f"[GIT] current branch {current_branch}")

            # get diff info
            default_branch = repo.git.symbolic_ref("refs/remotes/origin/HEAD")
            log_options = ["-p", f"{default_branch}..{current_branch}"]
            commits = repo.git.log(*log_options)
            return commits
        else:
            logger.error("[GIT] Path is not a git repository {path}")
            return ""
    except Exception as err:
        logger.error(f"[GIT] Unexpected error: {str(err)}")
        raise err


def parse_added_commit_changes(
    commits: str,
    commit_pattern: Pattern,
    author_pattern: Pattern,
    date_pattern: Pattern,
    file_pattern: Pattern,
) -> List[Dict[str, Any]]:
    commit_hashes = re.findall(commit_pattern, commits)
    author_sections = re.findall(author_pattern, commits)
    date_sections = re.findall(date_pattern, commits)

    commit_sections = re.split(commit_pattern, commits)
    commit_sections.pop(0)

    sections = []

    for i, section in enumerate(commit_sections):
        section_dict = {}

        commit_hash = commit_hashes[i].split(" ")[1]
        author = author_sections[i].strip().split(": ")[1]
        date = date_sections[i].strip().split(": ")[1]

        section = re.sub(commit_pattern, "", section)
        section = re.sub(author_pattern, "", section)
        section = re.sub(date_pattern, "", section)
        file_sections = re.split(file_pattern, section)
        file_sections.pop(0)

        added_changes = []
        for _j, file_section in enumerate(file_sections):
            added_changes.extend(
                re.findall(r"^\+[^\+].*", file_section, flags=re.MULTILINE)
            )

        section_dict["commit_hash"] = commit_hash
        section_dict["author"] = author
        section_dict["date"] = date
        section_dict["added_changes"] = added_changes

        # Append the dictionary to the list
        sections.append(section_dict)

    return sections
