NAME = $(shell appv name)
VERSION = $(shell appv version)
IMAGE = $(shell appv image)

test:
	sh run-tests.sh

build:
	docker build -t $(IMAGE) .

build-test:
	docker build -t "$(NAME)-test" -f Dockerfile.test .

deploy:
	kubectl apply -f deployment/
