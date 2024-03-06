.PHONY: grafana

POD_NAME := NAME

grafana:
	helm repo add grafana https://grafana.github.io/helm-charts
	helm repo update
	helm install grafana grafana/grafana -f values.yaml --namespace grafana-dashboard --create-namespace

grafana-port-forward:
	@export GRAFANA_POD_NAME=$$(kubectl get pods --namespace grafana-dashboard -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=grafana" -o jsonpath="{.items[0].metadata.name}") && \
	kubectl --namespace grafana-dashboard port-forward $$GRAFANA_POD_NAME 3000

clean:
	helm uninstall grafana --namespace grafana-dashboard