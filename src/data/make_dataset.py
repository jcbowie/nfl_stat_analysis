# -*- coding: utf-8 -*-
import logging
import requests
import pandas as pd
from pathlib import Path
from dotenv import find_dotenv, load_dotenv


url_list = []
for year in range(2009,2019):
    concat_url = "http://www.nfl.com/feeds-rs/schedules/{}.json".format(year)
    game_id_url = "http://www.nfl.com/feeds-rs/scores/byGame/"
    response = requests.get(concat_url)
    results = response.json()
    games = results['gameSchedules']
    game_ids = ["{}{}.json".format(game_id_url, item[-1]) for game in games for item in game.items() if item[0] == "gameId"]
    url_list.extend(game_ids)


def parse_data(url):
    row = []
    g_response = requests.get(url)
    g_results = g_response.json()
    for sect in g_results.items():
        if sect[0] == 'gameSchedule':
            for item in sect[-1].items():
                if item[0] == "visitorTeam":
                    continue
                elif item[0] == "homeTeam":
                    for it in item[-1].items():
                        if it[0] == "teamId":
                            team_id = it[-1]
                elif item[0] == "gameId":
                    game_id = item[-1]
                elif item[0] == "site":
                    for it in item[-1].items():
                        row.append(it[-1])
                else:
                    row.append(item[-1])
        else:
            for item in sect[-1].items():
                if (item[0] == 'homeTeamScore') | (item[0] == 'visitorTeamScore'):
                    tot = item[-1].get('pointTotal')
                    row.append(tot)
    stat_url = "http://www.nfl.com/feeds-rs/teamGameStats/{}/{}.json".format(team_id, game_id)
    s_response = requests.get(stat_url)
    s_results = s_response.json()
    home_team_stat_line = s_results['teamStatDetail']
    visitor_team_stat_line = s_results['opponentStatDetail']
    teams_stats = [home_team_stat_line, visitor_team_stat_line]
    for team in teams_stats:
        for item in team.items():
            row.append(item[-1])
    return row


def get_columns(url):
    row = []
    g_response = requests.get(url)
    g_results = g_response.json()
    for sect in g_results.items():
        if sect[0] == 'gameSchedule':
            for item in sect[-1].items():
                if item[0] == "visitorTeam":
                    continue
                elif item[0] == "homeTeam":
                    for it in item[-1].items():
                        if it[0] == "teamId":
                            team_id = it[-1]
                elif item[0] == "gameId":
                    game_id = item[-1]
                elif item[0] == "site":
                    for it in item[-1].items():
                        row.append(it[0])
                else:
                    row.append(item[0])
        else:
            for item in sect[-1].items():
                if (item[0] == 'homeTeamScore') | (item[0] == 'visitorTeamScore'):
                    row.append(item[0])
    stat_url = "http://www.nfl.com/feeds-rs/teamGameStats/{}/{}.json".format(team_id, game_id)
    s_response = requests.get(stat_url)
    s_results = s_response.json()
    home_team_stat_line = s_results['teamStatDetail']
    visitor_team_stat_line = s_results['opponentStatDetail']
    teams_stats = [home_team_stat_line, visitor_team_stat_line]
    for team in teams_stats:
        for item in team.items():
            row.append(item[0])
    return row


def main(urls):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    data = []
    counter = 0
    for l in urls[1:]:
        try:
            row = parse_data(l)
            data.append(row)
            counter += 1
            print(counter, "out of", len(url_list), "completed...")
        except:
            print("Skipped...")

    url_for_cols = urls[0]
    all_columns = get_columns(url_for_cols)
    game_columns = all_columns[:24]
    stat_columns = all_columns[24:]
    cols = stat_columns[:109]

    home_columns = []
    for col in cols:
        home_columns.append("H{}".format(col))

    visitor_columns = []
    for col in cols:
        visitor_columns.append("V{}".format(col))

    final_columns = []
    final_columns.extend(game_columns)
    final_columns.extend(home_columns)
    final_columns.extend(visitor_columns)

    df = pd.DataFrame(data)
    df.columns = final_columns
    df.to_csv("../data/interim/game_data.csv", index=False)

    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    return df


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    # load_dotenv(find_dotenv())

    main(url_list)
