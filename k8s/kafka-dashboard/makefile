.PHONY: kafka-dashboard

kafka-dashboard:
	helm repo add kafka-ui https://provectus.github.io/kafka-ui-charts
	helm install kafka-ui kafka-ui/kafka-ui -f values.yml --namespace kafka-cluster

kafka-dashboard-port-forward:
	@export KAFKA_UI_POD_NAME=$$(kubectl get pods --namespace kafka-cluster -l "app.kubernetes.io/name=kafka-ui,app.kubernetes.io/instance=kafka-ui" -o jsonpath="{.items[0].metadata.name}") && \
	kubectl --namespace kafka-cluster port-forward $$KAFKA_UI_POD_NAME 10000:8080
clean:
	helm uninstall kafka-ui --namespace kafka-cluster