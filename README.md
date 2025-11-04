## HDFS CSI Plugin

This repository contains a Spring Boot application configured to act as a CSI (Container Storage Interface) plugin for
Kubernetes with an HDFS integration. It supports running as a native GraalVM image for optimized performance and fast
startup.
This setup provides instructions to build the project, test it, and deploy it to Kubernetes using a **Helm chart**.

### Prerequisites

Before starting, ensure you have the following tools installed and configured in your development environment:

- **Java 17** (JDK)
- **Maven** (`>= 3.8.0`)
- **GraalVM** (for native builds)
- **Docker** (`>= 20.x`)
- **Helm** (`>= 3.x`)
- **Kubernetes** (cluster available and configured via `kubectl`)

### Build the Project

#### Build as a Standard JAR

You can build and package the project as a standard **Spring Boot JAR**:

``` bash
mvn clean package
```

This generates the JAR file under the `target/` directory:

``` bash
target/kubernetes-csi-1.0.0-SNAPSHOT.jar
```

#### Build as a Native GraalVM Executable

To build the project as a **GraalVM native image**, ensure that GraalVM is installed and `native-image` is available.
Then use the Maven profile `native`:

``` bash
mvn clean package -Pnative
```

This creates a native executable in the `target/` directory, significantly reducing startup time and memory usage:

``` bash
target/kubernetes-csi
```

### Run the Project Locally

After building the JAR file or native executable, you can run the application locally by providing HDFS configuration
via environment variables or configuration files.

#### Using the JAR File

``` bash
java -jar target/kubernetes-csi-1.0.0-SNAPSHOT.jar
```

#### Using the Native Executable

``` bash
./target/kubernetes-csi
```

#### Setting Configuration via Environment Variables

You can specify HDFS configurations using environment variables (default values are shown below):

``` bash
export HDFS_URL=hdfs://namenode:8020
export HDFS_USER=hdfs_user
```

### Build and Push the Docker Image

If deploying to Kubernetes, the application needs to be containerized. Use the provided `Dockerfile` to create an image.

1. **Build the Docker image:**

``` bash
   docker build -t your-docker-repo/hdfs-csi-plugin:latest .
```

1. **Push the image to your container registry:**

``` bash
   docker push your-docker-repo/hdfs-csi-plugin:latest
```

### Deploy to Kubernetes using Helm

The Helm chart in this repository makes it easy to deploy the CSI plugin to any Kubernetes cluster. Follow the
instructions below to deploy it.

#### Step 1: Update Helm Values (Optional)

The default configuration for the Helm chart is stored in `values.yaml`. You can override these values at install time
or update the `values.yaml` file directly.
Example `values.yaml` overrides:

``` yaml
hdfs:
  url: "hdfs://your-hdfs-url:8020"
  user: "your-hdfs-user"

image:
  repository: your-docker-repo/hdfs-csi-plugin
  tag: latest
```

#### Step 2: Install the Chart

To deploy the application to your Kubernetes cluster, run the following command:

``` bash
helm upgrade --install hdfs-csi-plugin ./hdfs-csi-plugin --namespace default
```

- `hdfs-csi-plugin`: The release name.
- `./hdfs-csi-plugin`: Path to the Helm chart (replace with the appropriate path).

This command deploys the following resources in Kubernetes:

- A for the HDFS configuration. `ConfigMap`
- A `Deployment` for the CSI plugin pods.
- A `Service` (optional) for exposing the plugin.

#### Step 3: Verify the Deployment

Check if the pod is running successfully:

``` bash
kubectl get pods -n default
```

For log output, use:

``` bash
kubectl logs <pod-name> -n default
```

### Uninstall the Helm Release

To remove the deployment from Kubernetes, run:

``` bash
helm uninstall hdfs-csi-plugin --namespace default
```

### Testing the Deployment

Once deployed, the plugin should connect to your specified HDFS system. Use the logs to verify connectivity and
actions (volume creation, deletion, etc.).
For example, logs should confirm successful connection to HDFS:

``` text
Connexion à HDFS réussie avec l'URL : hdfs://namenode:8020 et l'utilisateur : hdfs_user
```

### Configuration

The application reads the configuration for HDFS from a in Kubernetes and exposes the following environment variables:
`ConfigMap`

- `HDFS_URL`: The HDFS endpoint (e.g., `hdfs://namenode:8020`).
- `HDFS_USER`: The HDFS user for authentication.

These variables can be set dynamically through the Helm values or directly in the Kubernetes . `ConfigMap`

### Helm Chart Structure

The Helm chart has the following structure:

``` 
hdfs-csi-plugin/
├── Chart.yaml            # Used to define metadata about the chart
├── values.yaml           # Default values to configure the chart
└── templates/            # Kubernetes templates
    ├── configmap.yaml    # HDFS configuration data
    ├── deployment.yaml   # Deployment resource for the application
    └── service.yaml      # (Optional) Service to expose the plugin
```

### Additional Resources

- Kubernetes CSI Documentation: [https://kubernetes-csi.github.io/](https://kubernetes-csi.github.io/)
- Helm Documentation: [https://helm.sh/docs/](https://helm.sh/docs/)
- GraalVM Native Image: [https://www.graalvm.org/](https://www.graalvm.org/)

By following this guide, you'll be able to build, test, and deploy the HDFS CSI plugin efficiently across different
environments. If you encounter any issues, feel free to reach out or open an issue. 🚀
