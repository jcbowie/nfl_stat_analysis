import requests
import pandas as pd

save_path = input("Where would you like the file to be saved?: ")

url_list = []
for year in range(2019,2020):
    url = "http://www.nfl.com/feeds-rs/schedules/{}.json".format(year)
    game_id_url = "http://www.nfl.com/feeds-rs/scores/byGame/"
    response = requests.get(url)
    results = response.json()
    games = results['gameSchedules']
    game_ids = []
    game_ids = ["{}{}.json".format(game_id_url, item[-1]) for game in games for item in game.items() if item[0] == "gameId"]
    url_list.extend(game_ids)

    
def parse_data(url):
    row = []
    g_response = requests.get(url)
    g_results = g_response.json()
    for sect in g_results.items():
        if sect[0] == 'gameSchedule':
            for item in sect[-1].items():
                if (item[0] == "visitorTeam"):
                    continue
                elif (item[0] == "homeTeam"):
                    for it in item[-1].items():
                        if it[0] == "teamId":
                            team_id = it[-1]
                elif (item[0] == "gameId"):
                    game_id = item[-1]
                elif (item[0] == "site"):
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


def main(games_list):
    data = []
    counter = 0
    for game in games_list:
        row = []
        g_response = requests.get(game)
        g_results = g_response.json()
        g_schedule = g_results['gameSchedule']
        for item in g_schedule.items():
            if (item[0] == 'visitorTeam') | (item[0] == 'homeTeam') | (item[0] == 'site'):
                for it in item[-1].items():
                    point = it[-1]
                    row.append(point)
            else:
                row.append(item[-1])
        counter += 1        
        print("{} out of {}".format(counter, len(games_list)))
        data.append(row)
    columns = []
    g_response = requests.get(game)
    g_results = g_response.json()
    g_schedule = g_results['gameSchedule']
    for item in g_schedule.items():
        if (item[0] == 'visitorTeam') | (item[0] == 'homeTeam') | (item[0] == 'site'):
            for it in item[-1].items():
                point = it[0]
                columns.append(point)
        else:
            columns.append(item[0])
    df = pd.DataFrame(data)
    df.columns = columns
    
    return df


if __name__ == '__main__':

    df = main(url_list)
    df.to_csv(save_path, index=False)

    # ../../data/interim/2019_schedule.csv
