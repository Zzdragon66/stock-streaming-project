.PHONY: k8s-dashboard clean-dashboard

k8s-dashboard:
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml > /dev/null
	kubectl apply -f dashboard-adminuser.yaml > /dev/null
	kubectl get secret admin-user -n kubernetes-dashboard -o jsonpath={".data.token"} | base64 -d && echo
	kubectl proxy
	
clean-dashboard:
	kubectl -n kubernetes-dashboard delete serviceaccount admin-user
	kubectl -n kubernetes-dashboard delete clusterrolebinding admin-user
	kubectl delete namespace kubernetes-dashboard