# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string
import re

if sys.version_info[0] < 3:
    from HTMLParser import HTMLParser
    from urllib import unquote
    html_unescape = HTMLParser().unescape
else:
    from html import unescape as html_unescape
    from urllib.parse import unquote

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

from tornado import httpclient

import dns.exception
import dns.resolver
import requests
import json

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

class HigherVersion:
    def __init__(self, expression, min_version):
        self._expression = re.compile(expression)
        self._min_version = min_version

    def search(self, text):
        for result in self._expression.findall(text):
            if result >= self._min_version:
                print('Found version {}.'.format(result))
                return True

WGET_AT = find_executable(
    'Wget+AT',
    HigherVersion(
        r'(GNU Wget 1\.[0-9]{2}\.[0-9]{1}-at\.[0-9]{8}\.[0-9]{2})[^0-9a-zA-Z\.-_]',
        'GNU Wget 1.21.3-at.20260319.01'
    ),
    [
        './wget-at',
        '/home/warrior/data/wget-at-nss'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20260727.01'
TRACKER_ID = 'recursive'
TRACKER_HOST = 'legacy-api.arpa.li'
MULTI_ITEM_SIZE = 100
DNS_SERVERS = [
    '9.9.9.10',
    '149.112.112.10',
    '2620:fe::10',
    '2620:fe::fe:10'
]

dns_resolver = dns.resolver.Resolver(configure=False)
dns_resolver.nameservers = DNS_SERVERS
dns_resolver.cache = dns.resolver.Cache()


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            #ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 5:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        item_name_hash = hashlib.sha1(item_name.encode('utf8')).hexdigest()
        escaped_item_name = item_name_hash
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            item_name_hash,
            time.strftime('%Y%m%d-%H%M%S')
        ])
        job_config = json.loads(item['job_config'])
        if job_config.get('id'):
            split_id = job_config['id']
            if 'slug' in job_config:
                split_id = job_config['slug'] + '_' + split_id
            item['warc_file_base'] += '.split-' + split_id

        open('%(item_dir)s/%(warc_file_base)s.warc.gz' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_data.txt' % item, 'w').close()

class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.gz' % item,
              '%(data_dir)s/%(warc_file_base)s.warc.gz' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s_data.txt' % item,
              '%(data_dir)s/%(warc_file_base)s_data.txt' % item)

        shutil.rmtree('%(item_dir)s' % item)


class SortAndSelectItems(SimpleTask):
    ORDER = []
    ITEMS = {}

    def __init__(self):
        SimpleTask.__init__(self, 'SortAndSelectItems')

    @classmethod
    def handle_items(cls, items, log_output):
        counts = {}
        for item in items:
            item_job, item_value = item.split(':', 1)
            if item_job not in cls.ITEMS:
                cls.ORDER.append(item_job)
                cls.ITEMS[item_job] = []
            cls.ITEMS[item_job].append(item)
            if item_job not in counts:
                counts[item_job] = 0
            counts[item_job] += 1
        for k, v in sorted(counts.items()):
            log_output('Found {} items for job {}.'.format(v, k))
        selected_job = cls.ORDER.pop(0)
        log_output('Selecting all items for job {}.'.format(selected_job))
        items = cls.ITEMS[selected_job]
        del cls.ITEMS[selected_job]
        return selected_job, items

    def process(self, item):
        current_items = item['item_name'].split('\0')
        job, items = self.handle_items(current_items, item.log_output)
        item['item_job'] = job
        item['item_name'] = '\0'.join(items)


class GetJobConfig(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'GetJobConfig')

    def process(self, item):
        prop = 'job:{}:config'.format(item['item_job'])
        config = requests.get(
            'https://legacy-api.arpa.li/recursive/props',
            params={
                'prop': prop
            }
        ).json()[prop]
        item['job_config'] = json.dumps(json.loads(config))
        item.log_output('Got config for job {}.'.format(item['item_job']))


class SetBadUrls(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'SetBadUrls')

    def process(self, item):
        item['item_name_original'] = item['item_name']
        items = item['item_name'].split('\0')
        items_lower = [normalize_url(item['item_job']+':'+url, with_job=True) for url in json.loads(item['job_urls'])]
        with open('%(item_dir)s/%(warc_file_base)s_bad-items.txt' % item, 'r') as f:
            for url in {normalize_url(url, with_job=True) for url in f}:
                index = items_lower.index(url)
                items.pop(index)
                items_lower.pop(index)
        item['item_name'] = '\0'.join([s for s in items])


def normalize_url(url, with_job=False):
    if with_job:
        job, url = url.split(':', 1)
    url = url.split('#')[0]
    while True:
        temp = unquote(url).strip().lower()
        if temp == url:
            break
        url = temp
    if url.count('/') < 3:
        url += '/'
    url = url.split('/', 3)
    if '@' in url[2]:
        url[2] = url[2].split('@')[-1]
    if ':' in url[2]:
        url[2] = url[2].split(':')[0]
    url[0] = ''
    url = '/'.join(url)
    if with_job:
        url = job + ':' + url
    return url


class MaybeSendDoneToTracker(SendDoneToTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0:
            return self.complete_item(item)
        return super(MaybeSendDoneToTracker, self).enqueue(item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'recursive.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_AT,
            #'-U', USER_AGENT,
            '-nv',
            #'--no-cookies',
            '--host-lookups', 'dns',
            '--hosts-file', '/dev/null',
            '--resolvconf-file', '/dev/null',
            '--dns-servers', ','.join(DNS_SERVERS),
            '--reject-reserved-subnets',
            #'--prefer-family', ('IPv4' if 'PREFER_IPV4' in os.environ else 'IPv6'),
            '--content-on-error',
            '--lua-script', 'recursive.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--recursive', '--level=inf',
            '--no-parent',
            '--page-requisites',
            '--timeout', '30',
            '--connect-timeout', '1',
            '--tries', 'inf',
            '--span-hosts',
            '--waitretry', '30',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'x-wget-at-project-version: ' + VERSION,
            '--warc-header', 'x-wget-at-project-name: ' + TRACKER_ID,
            '--warc-dedup-url-agnostic',
            '--impersonate', 'firefox148-h1',
            '--header', 'Accept-Encoding: identity'
        ]

        if '--concurrent' in sys.argv:
            concurrency = int(sys.argv[sys.argv.index('--concurrent')+1])
        else:
            concurrency = os.getenv('CONCURRENT_ITEMS')
            if concurrency is None:
                concurrency = 2
            else:
                concurrency = int(concurrency)
        item['concurrency'] = str(concurrency)

        job_config = json.loads(item['job_config'])
        start_delay = 0
        if 'sleep_time' in job_config and 'inner' in job_config['sleep_time']:
            start_delay = random.uniform(0, job_config['sleep_time']['inner'] * concurrency)

        wget_args.extend(['--warc-header', 'job: '+item['item_job']])

        item['job_urls'] = []

        domains = set()

        for k in ('reject_subnets', 'prefer_subnets', 'defer_subnets', 'accept_subnets'):
            if k in job_config:
                wget_args.extend(['--'+k.replace('_', '-'), ','.join(job_config[k])])

        for item_name in item['item_name'].split('\0'):
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            wget_args.append('item-name://'+item_name)
            item_job, item_url = item_name.split(':', 1)
            assert item['item_job'] == item_job
            item_url = html_unescape(item_url)
            if '\\' in item_url or '%5C' in item_url or '%5c' in item_url:
                decoded = unquote(item_url).replace('\\/', '/')
                escaped = re.search(r'\\["\'](https?://[^\\]+|/[^\\]+)', decoded)
                if escaped:
                    item_url = escaped.group(1)
                    if item_url.startswith('/'):
                        item_url = re.match(r'^(https?://[^/]+)', decoded).group(1) + item_url
            item['job_urls'].append(item_url)
            try:
                if item_url.startswith('https://') \
                    and any(
                        answer.address == '209.202.252.66'
                        for answer in dns_resolver.resolve(
                            normalize_url(item_url).split('/', 3)[2],
                            'A',
                            search=False
                        )
                    ):
                    item.log_output('Switching {} to http://.'.format(item_url))
                    item_url = 'http:' + item_url.split(':', 1)[1]
            except dns.exception.DNSException:
                pass
            wget_args.append(item_url)
            #domains.add(normalize_url(item_url).split('/', 3)[2])

        #wget_args.extend(['--domains', ','.join(domains)])

        item['job_urls'] = json.dumps(item['job_urls'])

        item['item_name_newline'] = item['item_name'].replace('\0', '\n')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        time.sleep(start_delay)

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title=TRACKER_ID,
    project_html='''
        <img class="project-logo" alt="Project logo" src="" height="50px" title="https://wiki.archiveteam.org/images/thumb/7/77/ArchiveTeamWarriorLogo.png/235px-ArchiveTeamWarriorLogo.png"/>
        <h2>Recursive Jobs <span class="links"><a href="https://tracker.archiveteam.org/recursive/">Leaderboard</a> &middot; <a href="https://wiki.archiveteam.org/index.php/Distributed_recursive_crawls">Wiki</a></span></h2>
        <p>Running recursive Warrior crawl jobs.</p>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker('https://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION),
    SortAndSelectItems(),
    GetJobConfig(),
    PrepareDirectories(warc_prefix=TRACKER_ID),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'item_names': ItemValue('item_name_newline'),
            'warc_file_base': ItemValue('warc_file_base'),
            'concurrency': ItemValue('concurrency'),
            'item_job': ItemValue('item_job'),
            'job_config': ItemValue('job_config'),
            'job_urls': ItemValue('job_urls'),
            'dns_servers': json.dumps(DNS_SERVERS)
        }
    ),
    SetBadUrls(),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.gz')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='20',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        UploadWithTracker(
            'https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.warc.gz'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
