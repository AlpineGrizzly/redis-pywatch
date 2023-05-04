# redis-pywatch
Prints events occurring within Redis to stdout

## Usage
Connect to Redis on localhost on port 6379 (Default for redis) and subscribe to database 0. By default
if no database is specified, DB 0 will be chosen
```
python3 rediswatch.py 127.0.0.1 6379 0
python3 rediswatch.py 127.0.0.1 6379

```
