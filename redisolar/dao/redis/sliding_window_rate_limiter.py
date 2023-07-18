import datetime
import random

from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase, RateLimitExceededException
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""

    def __init__(
        self,
        window_size_ms: float,
        max_hits: int,
        redis_client: Redis,
        key_schema: KeySchema = None,
        **kwargs,
    ):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        key = self.key_schema.sliding_window_rate_limiter_key(
            name, self.window_size_ms, self.max_hits
        )
        pipeline = self.redis.pipeline()

        # Add an entry to the sorted set with the current timestamp in milliseconds as its score
        current_time_ms = datetime.datetime.utcnow().timestamp() * 1000

        pipeline.zadd(
            key,
            mapping={
                f"{current_time_ms}-{random.randint(0, 1000000)}": current_time_ms
            },
        )

        # Remove all entries from the sorted set that are older than CURRENT_TIMESTAMP - WINDOW_SIZE
        pipeline.zremrangebyscore(key, 0, current_time_ms - self.window_size_ms)

        # return the number of elements in the set
        pipeline.zcard(key)

        _, _, hits = pipeline.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException
