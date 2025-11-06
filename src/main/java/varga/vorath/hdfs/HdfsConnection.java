package varga.vorath.hdfs;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.util.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class HdfsConnection {

    private static final Logger logger = LoggerFactory.getLogger(HdfsConnection.class);

    private final Configuration configuration; // Isolated Hadoop configuration
    private final UserGroupInformation userGroupInformation; // Isolated authentication context

    /**
     * Constructor for the HdfsConnection.
     *
     * @param hdfsUri         The HDFS URI (e.g., hdfs://cluster1:8020).
     * @param keytabPath      The path to the Kerberos keytab.
     * @param principal       The Kerberos principal.
     * @param coreSiteContent Content to core-site.xml
     * @param hdfsSiteContent Content to hdfs-site.xml
     * @throws IOException If there is an issue loading configuration or performing authentication.
     */
    public HdfsConnection(String hdfsUri, String keytabPath, String principal, String coreSiteContent, String hdfsSiteContent) throws IOException {

        // Step 1: Load Hadoop configuration files
        this.configuration = new Configuration();

        // Add "core-site.xml" content
        if (coreSiteContent != null && !coreSiteContent.isEmpty()) {
            ByteArrayInputStream coreSiteStream = new ByteArrayInputStream(coreSiteContent.getBytes(StandardCharsets.UTF_8));
            this.configuration.addResource(coreSiteStream);
        }

        // Add "hdfs-site.xml" content
        if (hdfsSiteContent != null && !hdfsSiteContent.isEmpty()) {
            ByteArrayInputStream hdfsSiteStream = new ByteArrayInputStream(hdfsSiteContent.getBytes(StandardCharsets.UTF_8));
            this.configuration.addResource(hdfsSiteStream);
        }

        this.configuration.set("fs.defaultFS", hdfsUri); // Configure cluster URI

        // Step 2: Set up Kerberos authentication
        this.configuration.set("hadoop.security.authentication", "kerberos"); // Enable Kerberos in local config
        UserGroupInformation.setConfiguration(this.configuration); // Does not affect global state

        // Step 3: Perform user authentication using provided keytab and principal
        this.userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
    }

    /**
     * Executes an operation in the context of the authenticated user.
     *
     * @param operation The operation to execute.
     * @throws IOException If there is an issue executing the operation.
     */
    public <T> T executeAsAuthenticated(Operation<T> operation) throws IOException {
        return userGroupInformation.doAs((java.security.PrivilegedAction<T>) () -> operation.run(configuration));
    }

    /**
     * Gets the per-cluster configuration.
     *
     * @return The Hadoop configuration.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Gets the authenticated user information.
     *
     * @return The UserGroupInformation instance.
     */
    public UserGroupInformation getUserGroupInformation() {
        return userGroupInformation;
    }

    interface Operation<T> {
        T run(Configuration configuration);
    }

    public static HdfsConnection createHdfsConnection(String secretName, String secretNamespace, String location) throws ApiException, IOException {
        // Step 3: Fetch and decode secret data
        Map<String, String> secretData = getSecret(secretName, secretNamespace);

        // Extract required fields from the secret
        String principal = new String(Base64.getDecoder().decode(secretData.get("principal")));
        String keytabContent = new String(Base64.getDecoder().decode(secretData.get("keytab")));
        String coreSiteContent = new String(Base64.getDecoder().decode(secretData.get("core-site.xml")));
        String hdfsSiteContent = new String(Base64.getDecoder().decode(secretData.get("hdfs-site.xml")));

        logger.info("Fetched secret data successfully: {}, {}, {}", keytabContent, coreSiteContent, hdfsSiteContent);
        HdfsConnection hdfsConnection = new HdfsConnection(location, keytabContent, principal, coreSiteContent, hdfsSiteContent);
        return hdfsConnection;
    }

    /**
     * Fetches a Kubernetes secret using the Kubernetes API.
     *
     * @param secretName      Name of the secret.
     * @param secretNamespace Namespace of the secret.
     * @return A map of data entries from the secret, decoded as strings.
     * @throws ApiException If there is an error interacting with the Kubernetes API.
     */
    private static Map<String, String> getSecret(String secretName, String secretNamespace) throws ApiException, IOException {
        logger.info("Fetching secret '{}' from namespace '{}'", secretName, secretNamespace);

        // Initialize Kubernetes API client
        ApiClient client = Config.defaultClient();
        CoreV1Api api = new CoreV1Api(client);

        // Get the secret
        V1Secret secret = api.readNamespacedSecret(secretName, secretNamespace, null);

        // Return the decoded secret data.
        return secret.getStringData(); // This returns base64-encoded data entries
    }
}
