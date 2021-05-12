import riotwatcher
from datetime import datetime
from collections import defaultdict
import pytz


class LeagueAPI:
    def __init__(self, api_key, region='euw1'):
        self.watcher = riotwatcher.LolWatcher(api_key)
        self.region = region

    def find_account_id(self, summoner_name):
        try:
            res = self.watcher.summoner.by_name(self.region, summoner_name)
            return res['accountId']
        except riotwatcher.ApiError:
            return None

    def get_deaths_by_date(self, account_id, from_date):
        start_time = int(from_date.timestamp()) * 1000
        start_index = 0
        found_all = False
        latest_date = from_date
        deaths_per_date = defaultdict(int)
        rate_limited = False
        while not found_all:
            try:
                res = self.watcher.match.matchlist_by_account(
                    self.region,
                    account_id,
                    begin_time=start_time,
                    begin_index=start_index
                )
                found_all = res['endIndex'] == res['totalGames']
                start_index = res['endIndex']
                for match in res['matches'][::-1]:
                    deaths = self.get_deaths_from_match(
                        account_id, 
                        match['gameId']
                    )
                    match_date = datetime.fromtimestamp(
                        match['timestamp'] / 1000,
                        tz=pytz.UTC
                    )
                    deaths_per_date[match_date.date()] += deaths
                    if match_date > latest_date:
                        latest_date = match_date
            except riotwatcher.ApiError as e:
                rate_limited = e.response.status_code != 404
                break
        if not rate_limited:
            latest_date = datetime.utcnow()
        return dict(deaths_per_date), latest_date, rate_limited

    def get_deaths_from_match(self, account_id, match_id):
        res = self.watcher.match.by_id(self.region, match_id)
        parts = res['participantIdentities']
        p_id = list(filter(lambda p: p['player']['accountId'] == account_id,
                           parts))[0]
        part_id = p_id['participantId']
        part = list(filter(lambda p: p['participantId'] == part_id,
                            res['participants']))[0]
        return part['stats']['deaths']
