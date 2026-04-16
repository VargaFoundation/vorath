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

### Docker

The Docker image is automatically built and pushed to GitHub Container Registry via GitHub Actions on every push to `main`:

```bash
docker pull ghcr.io/varga-foundation/vorath:latest
```

To build manually:

```bash
docker build -t vorath:latest .
```

### Deploy to Kubernetes using Helm

The Helm chart is available on GitHub Container Registry (OCI) and can be installed directly.

#### Install from the OCI registry

```bash
helm install vorath oci://ghcr.io/varga-foundation/charts/hdfs-csi-plugin --version 1.0.0 \
  --namespace vorath --create-namespace
```

#### Install from local sources

```bash
helm install vorath ./kubernetes -n vorath --create-namespace
```

#### Configuration

Override values at install time or create a `values.override.yaml`:

```yaml
hdfs:
  url: "hdfs://namenode:8020"
  user: "hdfs_csi"

image:
  repository: ghcr.io/varga-foundation/vorath
  tag: latest
```

```bash
helm install vorath oci://ghcr.io/varga-foundation/charts/hdfs-csi-plugin --version 1.0.0 \
  -f values.override.yaml -n vorath --create-namespace
```

This deploys the following resources:

- A `ConfigMap` for the HDFS configuration.
- A `DaemonSet` for the CSI plugin pods.
- A `Service` for the plugin.
- A `CSIDriver` registration.
- RBAC resources (ClusterRole, ClusterRoleBinding, ServiceAccount).

#### Verify the Deployment

```bash
kubectl get pods -n vorath
kubectl get csidrivers.storage.k8s.io
kubectl logs -l app=hdfs-csi-plugin -n vorath
```

#### Uninstall

```bash
helm uninstall vorath -n vorath
```

### CI/CD

GitHub Actions automatically:
1. Builds the Maven project and runs tests.
2. Builds and pushes the Docker image to `ghcr.io/varga-foundation/vorath`.
3. Packages and pushes the Helm chart to `oci://ghcr.io/varga-foundation/charts/hdfs-csi-plugin`.

Triggers: push/PR to `main`/`master`, or manual dispatch.

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


#### License

##### Update third parties license file

Update the content of the file NOTICE:

    mvn org.codehaus.mojo:license-maven-plugin:add-third-party@add-third-party

##### Update license header on files

Update license header on files

    mvn org.codehaus.mojo:license-maven-plugin:update-file-header@update-file-header