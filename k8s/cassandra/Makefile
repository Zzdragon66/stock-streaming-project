.PHONY: cassandra-cluster

cassandra-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm install cassandra bitnami/cassandra -f values.yaml --namespace cassandra-cluster --create-namespace

cassandra-cluster-local-connection:
	kubectl port-forward --namespace cassandra-cluster svc/cassandra 9042:9042

clean:
	helm uninstall cassandra --namespace cassandra-cluster
	kubectl delete namespace cassandra-cluster