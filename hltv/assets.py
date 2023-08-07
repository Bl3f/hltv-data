from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from dagster import asset, WeeklyPartitionsDefinition, AssetIn, TimeWindowPartitionMapping, MetadataValue, Definitions, \
    EnvVar, DagsterType
from dagster_gcp import ConfigurablePickledObjectGCSIOManager

from .ressources import GCSResource

partitions_def = WeeklyPartitionsDefinition(start_date="2015-10-05", day_offset=1)

HtmlValid = DagsterType(
    name="HtmlValid",
    type_check_fn=lambda _, value: "Just a moment" not in value,
)


@asset(partitions_def=partitions_def, dagster_type=HtmlValid, io_manager_key="io_manager")
def world_ranking_html(context) -> str:
    date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    url = f"https://www.hltv.org/ranking/teams/{date.year}/{date.strftime('%B').lower()}/{date.day}"
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }

    context.log.info(url)

    response = requests.get(url, headers=headers)

    return response.text


@asset(
    partitions_def=partitions_def,
    ins={
        "world_ranking_html": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(),
        )
    },
)
def world_ranking(context, world_ranking_html) -> pd.DataFrame:
    soup = BeautifulSoup(world_ranking_html, 'html.parser')

    output = []

    for team in soup.find_all('div', class_='ranked-team'):
        team_change = team.select_one('.change').text
        team_name = team.select_one('.teamLine .name').text
        team_points = team.select_one('.teamLine .points').text

        output.append({
            "team": team_name,
            "points": team_points,
            "change": team_change,
        })

    df = pd.DataFrame(output)

    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


defs = Definitions(
    assets=[world_ranking_html, world_ranking],
    resources={
        "io_manager": ConfigurablePickledObjectGCSIOManager(
            gcs_bucket="bdp-hltv",
            gcs=GCSResource(
                project="blef-data-platform",
                service_account_json=EnvVar("SERVICE_ACCOUNT_JSON"),
            )
        ),
    }
)
