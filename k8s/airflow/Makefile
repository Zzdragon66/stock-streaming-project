.PHONY: airflow clean-airflow

DOCKER_NAME := NAME
AIRFLOW_IMAGE_NAME := NAME
STOCK_API := API
CLIENT_ID := ID 
CLIENT_SECRET := SECRET

STOCK_GENERATOR_IMAGE := STOCK_GENERATOR_IMAGE
REDDIT_NEWS_IMAGE := REDDIT_NEWS_IMAGE
STOCK_PREDICTION_IMAGE := STOCK_PREDICTION

airflow:
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace \
	-f values.yaml \
	--set images.airflow.repository=${DOCKER_NAME}/${AIRFLOW_IMAGE_NAME} \
	--set images.airflow.pullPolicy=Always \
	--set images.airflow.tag=latest \
	--set "env[0].name=AIRFLOW_VAR_STOCK_API" \
  	--set "env[0].value=${STOCK_API}" \
	--set "env[1].name=AIRFLOW_VAR_CLIENT_ID" \
  	--set "env[1].value=${CLIENT_ID}" \
	--set "env[2].name=AIRFLOW_VAR_CLIENT_SECRET" \
  	--set "env[2].value=${CLIENT_SECRET}" \
	--set "env[3].name=AIRFLOW_VAR_STOCK_GENERATOR_IMAGE" \
	--set "env[3].value=${STOCK_GENERATOR_IMAGE}" \
	--set "env[4].name=AIRFLOW_VAR_REDDIT_NEWS_IMAGE" \
	--set "env[4].value=${REDDIT_NEWS_IMAGE}" \
	--set "env[5].name=AIRFLOW_VAR_STOCK_PREDICTION_IMAGE" \
	--set "env[5].value=${STOCK_PREDICTION_IMAGE}" \
	--timeout "25m"
	kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

update-airflow:
	helm uninstall airflow --namespace airflow
	make airflow

clean-airflow:
	helm uninstall airflow --namespace airflow
	kubectl delete namespace airflow
