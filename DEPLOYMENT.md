# Deployment

The deployment of this project is managed with Docker and Kubernetes on Google Cloud.

### Configure

First, ensure the docker daemon can authenticate to the container registry. To authenticate to `gcr.io`, the `gcloud` command has a subcommand to generate a Docker authentication config file.

Assuming you are authenticated to the `gcloud` utility with the account needed for pushing to the registry namespace:

```
$ gcloud auth configure-docker
```

### Build

Build the Docker container from the top level directory of the repository, providing a version number tag. Here `v1` is used but should be changed to reflect major changes in a production deployment. Here the container registry `gcr.io` and the namespace `clingen-dev` are used, but these should be changed if using a different registry and namespace.

```$shell
$ docker build -t gcr.io/clingen-dev/clinvar-raw-producer:v1 .
$ docker push gcr.io/clingen-dev/clinvar-raw-producer:v1
```

### Deploy to Kubernetes

The file `clinvar-raw-producer-deployment.yaml` is used to deploy to kubernetes. The `containers.image` field must be the same as what the image was tagged as in the build step above.

```
$ kubectl apply -f clinvar-raw-producer-deployment.yaml
```

If a pod for `clivar-raw-producer` is already running (`kubectl get pods`), it can be restarted with the new image by deleting the existing pod. Since the `clinvar-raw-producer` deployment in this YAML file has 1 replica and is set to automatically pull the new image on startup, if we delete the running pod, kubernetes will restart it with the new image.

Obtain the name of the pod, which starts with `clinvar-raw-producer`, and delete it:
```
$ kubectl get pods
$ kubectl delete pod <podname>
```
