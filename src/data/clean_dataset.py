import pandas as pd
from pprint import pprint as pp
import datetime as dt
from sklearn import preprocessing

df = pd.read_csv("../data/interim/game_data_full.csv")
df_reg = df.loc[df['seasonType'] == "REG"]

for col, col_data in df_reg.iteritems():
    nan_count = col_data.isna().sum()
    if nan_count > 0:
        if col == "siteState":
            continue
        else:
            df_reg = df_reg.drop(col, axis=1)
            
df_reg = df_reg.reset_index()
df_reg = df_reg.drop('index', axis=1)

mapping_dictionary = {
    '49ers': 1,
    'Bears': 2,
    'Bengals': 3,
    'Bills': 4,
    'Broncos': 5,
    'Browns': 6,
    'Buccaneers': 7,
    'Cardinals': 8,
    'Chargers': 9,
    'Chiefs': 10,
    'Colts': 11,
    'Cowboys': 12,
    'Dolphins': 13,
    'Eagles': 14,
    'Falcons': 15,
    'Giants': 16,
    'Jaguars': 17,
    'Jets': 18,
    'Lions': 19,
    'Packers': 20,
    'Panthers': 21,
    'Patriots': 22,
    'Raiders': 23,
    'Rams': 24,
    'Ravens': 25,
    'Redskins': 26,
    'Saints': 27,
    'Seahawks': 28,
    'Steelers': 29,
    'Texans': 30,
    'Titans': 31,
    'Vikings': 32,
    'INDOOR': 1,
    'OUTDOOR': 2,
    'RETRACTABLE': 3,
    'Arlington': 1,
    'Atlanta': 2,
    'Baltimore': 3,
    'Carson': 4,
    'Charlotte': 5,
    'Chicago': 6,
    'Cincinnati': 7,
    'Cleveland': 8,
    'Denver': 9,
    'Detroit': 10,
    'East Rutherford': 11,
    'Foxborough': 12,
    'Glendale': 13,
    'Green Bay': 14,
    'Houston': 15,
    'Indianapolis': 16,
    'Jacksonville': 17,
    'Kansas City': 18,
    'Landover': 19,
    'London': 20,
    'Los Angeles': 21,
    'Mexico City': 22,
    'Miami Gardens': 23,
    'Minneapolis': 24,
    'Nashville': 25,
    'New Orleans': 26,
    'Oakland': 27,
    'Opa Locka': 28,
    'Orchard Park': 29,
    'Philadelphia': 30,
    'Pittsburgh': 31,
    'San Diego': 32,
    'San Francisco': 33,
    'Santa Clara': 34,
    'Seattle': 35,
    'St. Louis': 36,
    'St.Louis': 37,
    'Tampa': 38,
    'Toronto': 39,
    'Twickenham': 40,
    nan: 23,
    'AZ': 12,
    'CA': 15,
    'CO': 21,
    'FL': 3,
    'GA': 2,
    'IL': 20,
    'IN': 5,
    'LA': 4,
    'MA': 14,
    'MD': 7,
    'MI': 16,
    'MN': 22,
    'MO': 18,
    'NC': 8,
    'NJ': 11,
    'NY': 19,
    'OH': 9,
    'ON': 24,
    'PA': 1,
    'TN': 17,
    'TX': 6,
    'WA': 10,
    'WI': 13,
    '12:30:00': 8,
    '13:00:00': 2,
    '16:05:00': 7,
    '16:15:00': 3,
    '16:25:00': 12,
    '16:30:00': 18,
    '19:00:00': 5,
    '19:10:00': 13,
    '19:20:00': 10,
    '19:30:00': 9,
    '20:00:00': 11,
    '20:15:00': 20,
    '20:20:00': 4,
    '20:25:00': 15,
    '20:30:00': 1,
    '20:40:00': 16,
    '22:15:00': 6,
    '22:20:00': 14,
    '23:35:00': 17,
    '9:30:00': 19
}

columns_to_keep = ['season', 'week', 'gameTimeEastern', 'homeNickname', 'visitorNickname', 'siteId', 'siteCity',
                   'siteState', 'roofType', 'visitorTeamScore', 'homeTeamScore', 'HfirstDownsTotal',
                   'HfirstDownsByRushing', 'HfirstDownsByPassing', 'HfirstdownsByPenalty', 'HthirdDownAttempted',
                   'HthirdDownMade', 'HfourthDownAttempted', 'HfourthDownMade', 'HoffensiveYardsTotal',
                   'HoffensiveAvgYardsPerPlay', 'HrushingPlays', 'HrushingYardsTotal', 'HrushingAvgYardsPerPlay',
                   'HrushingLong', 'HpassingYardsTotal', 'HpassingNetYards', 'HpassingAttempts', 'HpassingCompletions',
                   'HpassingInterceptions', 'HpassingAvgYardsPerPlay', 'HpassingSacks', 'HpassingPasserRating',
                   'HfieldGoalAttempted', 'HfieldGoalBlocked', 'HfieldGoalMade', 'HextraTwoPointAtt',
                   'HextraTwoPointMade', 'HtotalExtraPointAtt', 'HtotalExtraPointMade', 'HtouchdownsTotal',
                   'HtouchdownsRushing', 'HtouchdownsPassing', 'HtouchdownsDefensive', 'Hpenalties', 'HpenaltiesYards',
                   'HdefensiveInterceptions', 'HdefensiveInterceptionsTds', 'HdefensiveInterceptionsYards',
                   'HdefensiveSacks', 'HdefensiveSafeties', 'HdefensiveTotalTackles', 'HfumblesTotal', 'HfumblesLost',
                   'HkickReturns', 'HkickReturnsTouchdowns', 'HkickingFgAttMade1To19', 'HkickingFgAttMade20To29',
                   'HkickingFgAttMade30To39', 'HkickingFgAttMade40To49', 'HkickingFgAttMade50plus', 'HpuntReturns',
                   'HpuntReturnsTouchdowns', 'HreceivingReceptions', 'HreceivingTouchdowns', 'HreceivingLong',
                   'HreceivingYards', 'HreceivingFumbles', 'HrecevingAverageYards', 'HtotalTurnovers',
                   'HtotalPointsScored', 'HtimeOfPossSeconds', 'VfirstDownsTotal', 'VfirstDownsByRushing',
                   'VfirstDownsByPassing', 'VfirstdownsByPenalty', 'VthirdDownAttempted', 'VthirdDownMade',
                   'VfourthDownAttempted', 'VfourthDownMade', 'VoffensiveYardsTotal', 'VoffensiveAvgYardsPerPlay',
                   'VrushingPlays', 'VrushingYardsTotal', 'VrushingAvgYardsPerPlay', 'VrushingLong',
                   'VpassingYardsTotal', 'VpassingNetYards', 'VpassingAttempts', 'VpassingCompletions',
                   'VpassingInterceptions', 'VpassingAvgYardsPerPlay', 'VpassingSacks', 'VpassingPasserRating',
                   'VfieldGoalAttempted', 'VfieldGoalBlocked', 'VfieldGoalMade', 'VextraTwoPointAtt',
                   'VextraTwoPointMade', 'VtotalExtraPointAtt', 'VtotalExtraPointMade', 'VtouchdownsTotal',
                   'VtouchdownsRushing', 'VtouchdownsPassing', 'VtouchdownsDefensive', 'Vpenalties', 'VpenaltiesYards',
                   'VdefensiveInterceptions', 'VdefensiveInterceptionsTds', 'VdefensiveInterceptionsYards',
                   'VdefensiveSacks', 'VdefensiveSafeties', 'VdefensiveTotalTackles', 'VfumblesTotal', 'VfumblesLost',
                   'VkickReturns', 'VkickReturnsTouchdowns', 'VkickingFgAttMade1To19', 'VkickingFgAttMade20To29',
                   'VkickingFgAttMade30To39', 'VkickingFgAttMade40To49', 'VkickingFgAttMade50plus', 'VpuntReturns',
                   'VpuntReturnsTouchdowns', 'VreceivingReceptions', 'VreceivingTouchdowns', 'VreceivingLong',
                   'VreceivingYards', 'VreceivingFumbles', 'VrecevingAverageYards', 'VtotalTurnovers',
                   'VtotalPointsScored', 'VtimeOfPossSeconds']

columns_not_standardized = ['gameDate', 'gameTimeEastern', 'homeNickname', 'visitorNickname',
                            'siteCity', 'siteState', 'roofType']

def get_outcomes(df_row):
    if df_row['visitorTeamScore'] == df_row['homeTeamScore']:
        value = "D"
    elif df_row['visitorTeamScore'] > df_row['homeTeamScore']:
        value = "V"
    else:
        value = "H"
    return value


def main(df):
    df_clean = df[columns_to_keep]
    df_clean.to_csv("../data/processed/game_data_clean(reg).csv", index=False)

    for col, col_data in df_clean.iteritems():
        if df[col].dtype == object:
            if "_" in str(col_data[0]):
                df_clean = df_clean.drop(col, axis=1)

    df_mapped = df_clean.replace(mapping_dictionary)
    df_objects = df_mapped[columns_not_standardized]
    df_mapped = df_mapped.drop(columns_not_standardized, axis=1)
    df_mapped['outcomes'] = df_mapped.apply(get_outcomes, axis=1)
    df_outcomes = df_mapped['outcomes']
    df_mapped = df_mapped.drop('outcomes', axis=1)
    df_columns = df_mapped.columns


    scaler = preprocessing.MinMaxScaler()
    df_mapped_array = scaler.fit_transform(df_mapped)
    df_standard = pd.DataFrame(df_mapped_array)
    df_standard.columns = df_columns

    df_final = pd.merge(df_objects, df_standard, left_index=True, right_index=True)
    df_final = pd.merge(df_final, df_outcomes, left_index=True, right_index=True)
    
    return df_final


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    df_final = main(df_reg)
    df_final.to_csv("../data/processed/final_standardized_data.csv", index=False)