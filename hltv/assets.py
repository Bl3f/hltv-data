import random
import time
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from dagster import asset, WeeklyPartitionsDefinition, AssetIn, TimeWindowPartitionMapping, MetadataValue, Definitions, \
    EnvVar, DagsterType, TimeWindowPartitionsDefinition
from dagster_gcp import ConfigurablePickledObjectGCSIOManager

from .ressources import GCSResource

partitions_def = WeeklyPartitionsDefinition(start_date="2015-10-05", day_offset=1, end_offset=1)

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
        team_id = team.select_one('a.moreLink:first-child', href=True)['href']

        players = []

        for player in team.select('td.player-holder'):
            player_id = player.select_one('.pointer', href=True)['href']
            player_name = player.select_one('img.playerPicture')['title']
            player_nick = player.select_one('.nick').text
            country = player.select_one('.nick img')['title']
            country_isocode = player.select_one('.nick img')['src']

            players.append({
                "id": player_id,
                "name": player_name,
                "nick": player_nick,
                "country": country,
                "country_isocode": country_isocode,
            })

        output.append({
            "date": context.asset_partition_key_for_output(),
            "team": team_name,
            "team_id": team_id,
            "points": team_points,
            "change": team_change,
            "players": players,
        })

    df = pd.DataFrame(output)
    df.index += 1

    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset
def teams(context, world_ranking):
    df = pd.concat(world_ranking).reset_index()
    df = df[["level_0", "level_1", "team", "team_id", "points", "change"]]
    df.rename(columns={"level_0": "date", "level_1": "rank"}, inplace=True)

    df["points"] = df["points"].str.extract(r"(\d+)")

    context.add_output_metadata(
        metadata={
            "nb_rows": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset
def players(context, world_ranking):
    df = pd.concat(world_ranking)
    out = pd.json_normalize(df["players"])

    context.add_output_metadata(
        metadata={
            "nb_rows": len(out),
            "preview": MetadataValue.md(out.head().to_markdown()),
        }
    )

    return out


events_partitions_def = TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",
    start=datetime(2012, 1, 1),
    end_offset=1,
    fmt="%Y-%m-%d",
)


@asset(
    partitions_def=events_partitions_def,
    dagster_type=HtmlValid,
)
def events_html(context) -> List[str]:
    window = context.partition_time_window
    url = f"https://www.hltv.org/events/archive?startDate={window.start.strftime('%Y-%m-%d')}&endDate={(window.end - timedelta(days=1)).strftime('%Y-%m-%d')}&eventType=MAJOR&eventType=INTLLAN&eventType=ONLINE&prizeMin=50000&prizeMax=3000000"
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }

    responses = []

    while True:
        context.log.info(url)
        response = requests.get(url, headers=headers)

        soup = BeautifulSoup(response.text, 'html.parser')
        next_page = soup.select_one(".pagination-next:not(.inactive)")

        responses.append(response.text)

        if next_page:
            url = f"https://www.hltv.org/{next_page['href']}"
        else:
            break

    return responses


@asset(
    partitions_def=events_partitions_def,
    ins={
        "events_html": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(),
        )
    },
)
def events(context, events_html) -> pd.DataFrame:
    output = []
    for page in events_html:
        context.log.info(page)

        soup = BeautifulSoup(page, 'html.parser')

        for event in soup.select(".small-event"):
            context.log.info(event)
            href = event["href"]
            event_name = event.select_one("td.event-col").text
            nb_teams = event.select_one("tr > td:nth-child(2)").text
            prize_pool = event.select_one("td.prizePoolEllipsis").text
            event_type = event.select_one("td.gtSmartphone-only").text
            country = event.select_one("td .smallCountry").text
            dates = [date.text for date in event.select("span[data-unix]")]

            output.append({
                "event_name": event_name,
                "country": country,
                "nb_teams": nb_teams,
                "prize_pool": prize_pool,
                "event_type": event_type,
                "href": href,
                "dates": dates,
            })

    df = pd.DataFrame(output)

    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(df.head().to_markdown()),
            "nb_rows": MetadataValue.int(len(df)),
        }
    )

    return df


@asset(
    partitions_def=events_partitions_def,
    ins={
        "events": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(),
        )
    },
)
def events_details(context, events) -> pd.DataFrame:
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }

    placements = []

    for event_href in events["href"]:
        url = f"https://www.hltv.org{event_href}"

        retry = 0
        RETRY_MAX = 5
        while retry < RETRY_MAX:
            context.log.info(url)
            response = requests.get(url, headers=headers)

            if "Just a moment" in response.text:
                retry += 1
                context.log.info(f"Retrying... (n°{retry})")
                time.sleep(retry * 20)
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            try:
                for placement in soup.select(".placement"):
                    team = placement.select_one(".team a").text
                    team_href = placement.select_one(".team a")["href"]
                    rank = placement.select_one("div:nth-child(2)").text
                    prize = placement.select_one("div:nth-child(3)").text

                    placements.append({
                        "team": team,
                        "team_href": team_href,
                        "rank": rank,
                        "prize": prize,
                    })

                    time.sleep(random.randint(1, 20))

                retry = RETRY_MAX * 2
            except Exception as er:
                print(response.text)
                raise

    df = pd.DataFrame(placements)

    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(df.head().to_markdown()),
            "nb_rows": MetadataValue.int(len(df)),
        }
    )

    return df


defs = Definitions(
    assets=[world_ranking_html, world_ranking, players, teams, events_html, events, events_details],
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
