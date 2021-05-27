from praw.models import MoreComments
import os
import json
import praw
import time

def convertTime(human_time):
    # human_time should be like: 29.09.2013
    date_time = human_time + ' 00:00:01'
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int(time.mktime(time.strptime(date_time, pattern)))
    return epoch


class dataCrawler(object):
    """A class to crawl data from reddit"""
    def __init__(self, info_file, output_path, year='2017', subreddit_name='changemyview'):
        info = {}
        self.year = year
        self.output = output_path
        for line in open(info_file):
            lines = line.strip().split(':')
            info[lines[0]] = lines[1]
        self.reddit = praw.Reddit(client_id = info['client_id'],
                                  client_secret = info['client_secret'],
                                  user_agent = info['user_agent'],
                                  username = info['username'],
                                  password = info['password'])
        self.cmv = self.reddit.subreddit(subreddit_name)
        self.comments_fields = {'author', 'body', 'created_utc', 'created', 'depth',
                'downs', 'edited', 'fullname', 'is_root', 'likes', 'link_id', 'id', 'name',
                'parent_id', 'score', 'ups', 'controversiality'}
        self.submission_fields = {'url', 'permalink', 'title', 'author', 'selftext', 'fullname', 'subreddit_id', 'num_comments', 'score', 'ups', 'upvote_ratio', 'view_count', 'visited', 'downs' }

    def __getComment__(self, comment):
        cur_obj = {}
        for f in self.comments_fields:
            val = eval('comment.' + f)
            if type(val) == type(u'a'):
                val = val.encode('utf-8')
            cur_obj[f] = str(val)
        if len(comment.replies) == 0:
            cur_obj['replies'] = []
        else:
            cur_obj['replies'] = self.__getComments__(comment.replies, reply=True)
        return cur_obj


    def __getComments__(self, comments, reply=False):
        comment_lst = []
        for idx, comment in enumerate(comments):
            if isinstance(comment, MoreComments):
                print('MoreComments encountered.')
                continue
            #if not reply:
            #    print(idx,'/',len(comments),'number of replies:',len(comment.replies), 'link_id:',comment.link_id)
            cur_obj = self.__getComment__(comment)
            comment_lst.append(cur_obj)
        return comment_lst


    def crawl(self, start_date, end_date, limit=-1):
        start_date = '02.' + start_date + '.' + self.year
        end_date =  '01.' + end_date + '.' + self.year

        fout = open(self.output + 'start_' + start_date + '_end_' + end_date + '.jsonlist', 'w')
        submission_lst = list(self.cmv.submissions(
                              convertTime(start_date), convertTime(end_date)))
        print('start date: %s\tend date:%s\tnumber of posts:%d' % (start_date, end_date, len(submission_lst)))
        crawled_cnt = 0
        for idx, submission in enumerate(submission_lst):
            cur_obj = {}
            for f in self.submission_fields:
                val = eval('submission.' + f)
                if type(val) == type(u'a'):
                    val = val.encode('utf-8')
                cur_obj[f] = str(val)
            submission.comments.replace_more(limit=0)
            if idx % 20 == 0:
                print('year:%s\tprocessing %d th submission, number of comments: %d' % (self.year, idx, len(submission.comments)))
            cur_obj['comments'] = self.__getComments__(submission.comments, reply=False)
            fout.write(json.dumps(cur_obj) + '\n')
            crawled_cnt += 1
            if limit > 0 and crawled_cnt == limit:
               break
        fout.close()
        print('crawling finished.')
        return

class deltaLogCrawler(object):
    """A class to crawl delta log"""
    def __init__(self, info_file, output_path, year):
        self.output = output_path
        self.year = year
        info = {}
        for line in open(info_file):
            lines = line.strip().split(':')
            info[lines[0]] = lines[1]
        self.reddit = praw.Reddit(client_id = info['client_id'],
                                  client_secret = info['client_secret'],
                                  user_agent = info['user_agent'],
                                  username = info['username'],
                                  password = info['password'])
        self.cmv = self.reddit.subreddit('deltaLog')
        self.submission_fields = {'title','selftext', 'selftext_html','url', 'fullname', 'created', 'created_utc', 'id', 'permalink', 'quarantine', 'score'}

    def crawl(self, start_date, end_date):
        start_date = '02.' + start_date + '.' + self.year
        end_date =  '01.' + end_date + '.' + self.year

        fout = open(self.output + 'start_' + start_date + '_end_' + end_date + '.jsonlist', 'w')
        submission_lst = list(self.cmv.submissions(
                              convertTime(start_date), convertTime(end_date)))
        print('start date: %s\tend date:%s\tnumber of logs:%d' % (start_date, end_date, len(submission_lst)))
        for idx, submission in enumerate(submission_lst):
            print('processing %d th submission...' % idx)
            cur_obj = {}
            for f in self.submission_fields:
                val = eval('submission.' + f)
                if type(val) == type(u'a'):
                    val = val.encode('utf-8')
                cur_obj[f] = str(val)
            fout.write(json.dumps(cur_obj) + '\n')
        fout.close()
        print('crawling finished.')
 
if __name__=='__main__':
    year = 2019
    path = f'./data/posts/{year}/'
    if not os.path.exists(path):
        os.makedirs(path)

    dc = dataCrawler(info_file='info.txt', output_path=path, year=year)
    dc.crawl('01', '02')
