include .env
.PHONY: images

# K8s Directories 
K8S_DASH_DIR := ./k8s/k8s-dashboard
K8S_AIR_DIR := ./k8s/airflow
AIRFLOW_IMAGE_DIR := docker-service/airflow
K8S_KAFKA_CLUSTER := k8s/kafka
K8S_CASSANDRA_CLUSTER := k8s/cassandra
K8S_KAFKA_DASHBOARD := k8s/kafka-dashboard
K8S_GRAPHANA := k8s/grafana
K8S_SPARK := k8s/spark

# Specify the docker image name

AIRFLOW_IMAGE_NAME := stock-airflow

# Specify the architecture 

docker-builder-init:
	@if ! docker buildx ls | grep -q "stock_builder"; then \
		docker buildx create --name stock_builder --use; \
	else \
		echo "stock_builder already exists and in use"; \
	fi

airflow-image-arm64: docker-builder-init 
	cd $(AIRFLOW_IMAGE_DIR) && \
	docker buildx build --platform linux/arm64 -t $(dockerhub_username)/$(AIRFLOW_IMAGE_NAME):latest --build-arg ARCH=arm64 --push .

airflow-image-amd64: docker-builder-init 
	cd $(AIRFLOW_IMAGE_DIR) && \
	docker buildx build --platform linux/amd64 -t $(dockerhub_username)/$(AIRFLOW_IMAGE_NAME):latest --build-arg ARCH=amd64 --push .	

k8s-dashboard:
	cd $(K8S_DASH_DIR) && make k8s-dashboard

k8s-kafka:
	cd $(K8S_KAFKA_CLUSTER) && make kafka-cluster

k8s-cassandra:
	cd $(K8S_CASSANDRA_CLUSTER) && make cassandra-cluster

k8s-kafka-dashboard:
	cd $(K8S_KAFKA_DASHBOARD) && make kafka-dashboard

k8s-spark:
	cd $(K8S_SPARK) && make spark-cluster

k8s-data-dashboard:
	cd $(K8S_GRAPHANA) && make grafana


k8s-airflow-arm64: airflow-image-arm64
	cd $(K8S_AIR_DIR) && make airflow STOCK_API=$(stock_api) DOCKER_NAME=$(dockerhub_username) \
		AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) CLIENT_ID=$(reddit_client) CLIENT_SECRET=$(reddit_secret)

k8s-airflow-amd64: airflow-image-amd64
	cd $(K8S_AIR_DIR) && make airflow STOCK_API=$(stock_api) DOCKER_NAME=$(dockerhub_username) \
		AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) CLIENT_ID=$(reddit_client) CLIENT_SECRET=$(reddit_secret)

k8s-airflow-update-arm64: airflow-image-arm64
	cd $(K8S_AIR_DIR) && make update-airflow STOCK_API=$(stock_api) DOCKER_NAME=$(dockerhub_username) \
		AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) CLIENT_ID=$(reddit_client) CLIENT_SECRET=$(reddit_secret)

k8s-airflow-update-amd64: airflow-image-amd64
	cd $(K8S_AIR_DIR) && make update-airflow STOCK_API=$(stock_api) DOCKER_NAME=$(dockerhub_username) \
		AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) CLIENT_ID=$(reddit_client) CLIENT_SECRET=$(reddit_secret)


clean:
	cd $(K8S_DASH_DIR) && make clean-dashboard
	cd $(K8S_CASSANDRA_CLUSTER) && make clean
	cd $(K8S_KAFKA_CLUSTER) && make clean-kafka-cluster
	cd $(K8S_KAFKA_DASHBOARD) && make clean
	cd $(K8S_GRAPHANA) && make clean
	cd $(K8S_SPARK) && make clean-spark-cluster
	cd $(K8S_AIR_DIR) && make clean-airflow


