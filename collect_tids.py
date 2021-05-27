"""query cmv tids according to time stamp

To run:

    python collect_tids.py 2019 1 # crawl all thread ids for 2019-01 to 2019-02

"""
import json
import sys
import datetime as dt
from psaw import PushshiftAPI

args = sys.argv
year = int(args[1])
start_time = int(args[2])


fout = open("./data/tids-%d/tids_%d_%d.jsonlist" % (year, year, start_time), 'w')

start_epoch = int(dt.datetime(year, start_time, 1).timestamp())
if start_time == 12:
    end_epoch = int(dt.datetime(year + 1, 1, 1).timestamp())
else:
    end_epoch = int(dt.datetime(year, start_time + 1, 1).timestamp())

api = PushshiftAPI()
sub_lst = api.search_submissions(
        before=end_epoch,
        after=start_epoch,
        subreddit='changemyview')

print("start downloading tids")
cnt = 0
for item in sub_lst:
    cnt += 1
    cur_obj = dict()
    cur_obj['id'] = item.id
    cur_obj['title'] = item.title
    cur_obj['created_utc'] = item.created_utc
    cur_obj['selftext'] = item.selftext if 'selftext' in dir(item) else 'UNK'
    cur_obj['num_comments'] = item.num_comments
    cur_obj['author'] = item.author
    cur_obj['permalink'] = item.permalink
    cur_obj['url'] = item.url
    fout.write(json.dumps(cur_obj) + "\n")
fout.close()
print("finished for %d-%d" % (year, start_time))
print(cnt)
