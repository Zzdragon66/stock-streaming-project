.PHONY: kafka-cluster

kafka-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm install kafka bitnami/kafka -f values.yaml --namespace kafka-cluster --create-namespace

# helm repo add confluentinc https://packages.confluent.io/helm
# helm repo update
# helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace kafka --create-namespace

clean-kafka-cluster: 
	helm delete kafka --namespace kafka-cluster