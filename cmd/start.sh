SVC_NAME=$(jq '.name' -r ../appv.json)
SVC_VERSION=$(jq '.version' -r ../appv.json)
CONF_DIR="../integrationtest/conf"

export DB_HOST='127.0.0.1'
export DB_PORT='5432'
export DB_NAME='newsranker'
export DB_USERNAME='newsranker'
export DB_PASSWORD='newsranker'
export TWITTER_USERS='320000000'
export REFERENCE_WEIGHT='1000'
export MQ_EXCHANGE='x-news'
export MQ_RANK_QUEUE='q-rank-objects'
export MQ_SCRAPE_QUEUE='q-scrape-targets'
export MQ_SCRAPED_QUEUE='q-scraped-articles'
export MQ_HEALTH_TARGET='q-health-newsranker'
export MQ_HOST=$DB_HOST
export MQ_PORT='5672'
export MQ_USER='newsranker'
export MQ_PASSWORD='password'
export HEARTBEAT_FILE='/tmp/news-ranker-health.txt'
export HEARTBEAT_INTERVAL='20'

echo "Building $SVC_NAME"
go build

echo "Starting $SVC_NAME-$SVC_VERSION"
./cmd
