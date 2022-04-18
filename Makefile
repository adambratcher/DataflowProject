IMAGE_NAME ?= dataflow_template
TAG ?= dev
PROJECT ?= gcr.io/gs-development-168620

.PHONY: build run
run:    ./Dockerfile
	$$(docker image inspect ${IMAGE_NAME}:${TAG} > /dev/null 2>&1) || docker build -t ${IMAGE_NAME}:${TAG} -f ./Do$
	touch .container_bash_history
	docker-compose run --rm --name ${IMAGE_NAME} ${IMAGE_NAME} bash --rcfile .bashrc -i

build: ./Dockerfile
	docker build -t ${IMAGE_NAME}:${TAG} -f ./Dockerfile .
	touch .container_bash_history

tag:
	docker tag ${IMAGE_NAME}:${TAG} ${PROJECT}/dataflow_template:${TAG}

clean:
	docker container rm $$(docker container ls -aqf ancestor=${IMAGE_NAME}:${TAG})

push:
	docker push ${PROJECT}/dataflow_template:${TAG}

default: run
