.PHONY: spark-cluster

spark-cluster: # Initialize the spark-cluster
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm install spark bitnami/spark -f values.yaml --namespace spark-cluster --create-namespace

spark-port-forward:
	kubectl port-forward --namespace spark-cluster svc/spark-master-svc 10001:80

clean-spark-cluster:
	helm uninstall spark --namespace spark-cluster
	kubectl delete namespace spark-cluster