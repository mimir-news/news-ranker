NAME=news-ranker
VERSION=2.0-DEV
IMAGE="eu.gcr.io/mimir-123/$(NAME):$(VERSION)"

test:
	sh run-tests.sh

build:
	docker build -t $(IMAGE) .

build-test:
	docker build -t "$(NAME)-test" -f Dockerfile.test .
