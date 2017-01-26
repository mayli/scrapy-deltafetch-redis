import logging
import os

from scrapy.http import Request
from scrapy.item import BaseItem
from scrapy.utils.request import request_fingerprint
from scrapy.utils.python import to_bytes
from scrapy.exceptions import NotConfigured
from scrapy import signals

from redis.client import StrictRedis

logger = logging.getLogger(__name__)


class DeltaFetchRedis(object):
    """
    This is a spider middleware to ignore requests to pages containing items
    seen in previous crawls of the same spider, thus producing a "delta crawl"
    containing only new items.

    This also speeds up the crawl, by reducing the number of requests that need
    to be crawled, and processed (typically, item requests are the most cpu
    intensive).
    """

    def __init__(self, conn_url, reset=False, stats=None):
        self.db = None
        self.conn_url = conn_url
        self.reset = reset
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        if not s.getbool('DELTAFETCH_ENABLED'):
            raise NotConfigured
        if not s.get('DELTAFETCH_REDIS_URL'):
            raise NotConfigured
        conn_url = s.get('DELTAFETCH_REDIS_URL')
        reset = s.getbool('DELTAFETCH_RESET')
        o = cls(conn_url, reset, crawler.stats)

        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.dbkey = 'deltafetch.redis.%s' % spider.name
        reset = self.reset or getattr(spider, 'deltafetch_reset', False)

        self.db = StrictRedis.from_url(self.conn_url)
        assert self.db.echo("test") == "test", "Connection failed"
        if reset:
            self.db.delete(self.dbkey)

    def spider_closed(self, spider):
        self.db.bgsave()

    def process_spider_output(self, response, result, spider):
        for r in result:
            if isinstance(r, Request):
                key = self._get_key(r)
                if self.db.sismember(self.dbkey, key):
                    logger.info("Ignoring already visited: %s" % r)
                    if self.stats:
                        self.stats.inc_value('deltafetch/skipped', spider=spider)
                    continue
            elif isinstance(r, (BaseItem, dict)):
                key = self._get_key(response.request)
                self.db.sadd(self.dbkey, key)
                if self.stats:
                    self.stats.inc_value('deltafetch/stored', spider=spider)
            yield r

    def _get_key(self, request):
        key = request.meta.get('deltafetch_key') or request_fingerprint(request)
        # request_fingerprint() returns `hashlib.sha1().hexdigest()`, is a string
        return to_bytes(key)
