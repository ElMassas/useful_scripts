import json
import os
import sys
from collections import defaultdict
from datetime import date
from typing import Any, List

import requests
from loguru import logger


def save_language_data_to_json(languages: defaultdict, languages_sizes: defaultdict) -> bool:
    # Get the current date in YYYY-MM-DD format
    current_date = date.today()
    formatted_date = current_date.strftime('%Y_%B_%d')

    data = {
        "languages": languages,
        "languages_percentages": languages_sizes,
    }
    
    directory = "data/language_scans"
    os.makedirs(directory, exist_ok=True)
    
    filename = os.path.join(directory, f"language_data_{formatted_date}.json")
    
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    logger.info(f"Language info saved to file: {filename}")
    return True

def retrieve_projects_ids(git_url: str, auth_token: str) -> List:
    # API documentation: https://docs.gitlab.com/ee/api/projects.html#languages
    list_projects_url = f"{git_url}/api/v4/projects"
    params = {
        "private_token": auth_token,
        "per_page": 100, # max value returned by gitlab on free tier
    }
    page = 1
    ids = []

    while True:
        try:
            params["page"] = page
            response = requests.get(list_projects_url, params=params)

            if response.status_code == 200:
                data = response.json()

                if len(data) == 0:
                    break

                for project in data:
                    ids.append(project["id"])

                logger.debug(f"Retrieved ids from page: {page} with {params["per_page"]} ids per page")

                page += 1

            else:
                logger.info(
                    "Response status code",
                    url=list_projects_url,
                    code=response.status_code,
                )
                sys.exit(1)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return []
    logger.info(f"Retrieved {len(ids)} ids from gitlab")
    return ids


def calculate_languages_size(size: int, languages: dict) -> Any:
    languages_byte_size = {}
    for language, percentage in languages.items():
        languages_byte_size[language] = round((percentage / 100.0) * size)
    return languages_byte_size


def get_project_languages(project_id: str, git_url: str, auth_token: str) -> Any:
    try:
        project_languages = (
            f"https://git.worten.net/api/v4/projects/{project_id}/languages"
        )
        project_size = (
            f"https://git.worten.net/api/v4/projects/{project_id}?statistics=true"
        )
        headers = {"PRIVATE-TOKEN": auth_token}

        languages_response = requests.get(project_languages, headers=headers)
        size_response = requests.get(project_size, headers=headers)

        if languages_response.status_code == 200 and size_response.status_code == 200:
            if languages_response.json() == {}:
                logger.info(f"Project {project_id} has no languages")
                return None
            else:
                size = size_response.json()["statistics"]["repository_size"]
                logger.info(
                    f"Retrieved info: {languages_response.json()}, id: {project_id}, size: {size}"
                )
                return calculate_languages_size(size, languages_response.json())
        else:
            logger.info("Response status code", id=project_id)
            sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}", id=project_id)
        return None


def main():
    auth_token = "" # update here
    ids = retrieve_projects_ids(auth_token)

    ids_set = set(ids)

    languages = defaultdict(int)
    languages_sizes = defaultdict(int)

    for project_id in ids_set:
        project_languages = get_project_languages(project_id, auth_token)
        if project_languages is None:
            continue
        
        for language, size in project_languages.items():
            # Update overall language counts and sizes
            languages[language] += 1
            languages_sizes[language] += size

    total_bytes = sum(languages_sizes.values())

    total_languages_percentages = {
        language: round((size / total_bytes) * 100, 2)
        for language, size in languages_sizes.items()
    }

    # Sort languages by size
    sorted_languages_size = dict(
        sorted(total_languages_percentages.items(), key=lambda item: item[1], reverse=True)
    )

    # Sort languages by count
    sorted_languages = dict(
        sorted(languages.items(), key=lambda item: item[1], reverse=True)
    )

    save_language_data_to_json(sorted_languages, sorted_languages_size)


if __name__ == "__main__":
    main()
