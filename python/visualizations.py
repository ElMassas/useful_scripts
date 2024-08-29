import json
import os
from datetime import date
from typing import Any

import matplotlib.pyplot as plt
from apps.language_scan.utils.log import setup_logging
from loguru import logger


def get_top_20_items(data_dict):
    # Sort by value in descending order and return the top 20
    return dict(sorted(data_dict.items(), key=lambda item: item[1], reverse=True)[:20])


# Load JSON data
def load_data() -> Any:
    json_file_path = input("Please enter the path to the JSON file: ")

    if (
        not json_file_path.endswith(".json")
        or not os.path.isfile(json_file_path)
        or not json_file_path.startswith("data/")
    ):
        logger.error(
            f"Error: The file '{json_file_path}' either does not exist, is not a .json file, or is not located in the 'data' directory."
        )
        exit(1)

    with open(json_file_path, "r") as file:
        data = json.load(file)

    return data


def create_bar_chart(data_dict, title, output_path):
    languages = list(data_dict.keys())
    values = list(data_dict.values())

    # Create the bar chart
    plt.figure(figsize=(12, 8))
    plt.bar(languages, values, color="skyblue")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Count")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def create_pie_chart(data_dict, title, output_path):
    languages = list(data_dict.keys())
    sizes = list(data_dict.values())

    plt.figure(figsize=(8, 8))
    plt.pie(
        sizes,
        labels=languages,
        autopct="%1.1f%%",
        startangle=140,
        colors=plt.cm.Paired(range(len(data_dict))),
    )
    plt.axis("equal")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def generate_charts(
    top_languages: Any,
    top_languages_percentages: Any,
    output_dir: str,
):
    # Generate bar charts
    current_date = date.today()
    formatted_date = current_date.strftime("%Y_%B_%d")

    create_bar_chart(
        top_languages,
        "Programming languages count",
        os.path.join(output_dir, f"languages_bar_chart_{formatted_date}.png"),
    )

    # Generate pie charts
    create_pie_chart(
        top_languages_percentages,
        "Programming languages use percentages",
        os.path.join(
            output_dir, f"languages_percentages_pie_chart_{formatted_date}.png"
        ),
    )


def main():
    setup_logging(json=True)
    output_dir = "data/language_scans"
    os.makedirs(output_dir, exist_ok=True)
    data = load_data()
    top_languages = get_top_20_items(data["languages"])
    top_languages_percentages = get_top_20_items(data["languages_percentages"])

    generate_charts(
        top_languages,
        top_languages_percentages,
        output_dir,
    )
    logger.info("Charts created and saved successfully.")
