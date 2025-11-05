package varga.vorath.hdfs;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

public class HdfsVirtualFileSystem extends FuseStubFS {
    private static final Logger logger = LoggerFactory.getLogger(HdfsVirtualFileSystem.class);

    private FileSystem hdfs;
    private final String hdfsUri;
    private final String rootPath;

    public HdfsVirtualFileSystem(String hdfsUri, String rootPath) {
        this.hdfsUri = hdfsUri;
        this.rootPath = rootPath;
        initializeHdfsClient();
    }

    private void initializeHdfsClient() {
        try {
            Configuration config = new Configuration();
            config.set("fs.defaultFS", hdfsUri);
            this.hdfs = FileSystem.get(config);
            logger.info("Connected to HDFS: {}", hdfsUri);
        } catch (IOException e) {
            logger.error("Failed to connect to HDFS", e);
            throw new RuntimeException("HDFS connection failed", e);
        }
    }

    @Override
    public int getattr(String path, FileStat stat) {
        try {
            Path hdfsPath = getHdfsPath(path);

            if (hdfsPath.isRoot()) {
                stat.st_mode.set(FileStat.S_IFDIR | 0755); // Directory with permissions
                stat.st_nlink.set(2); // Number of links
                return 0;
            }

            FileStatus fileStatus = hdfs.getFileStatus(hdfsPath);
            if (fileStatus.isDirectory()) {
                stat.st_mode.set(FileStat.S_IFDIR | 0755); // It's a directory
            } else {
                stat.st_mode.set(FileStat.S_IFREG | 0644); // Regular file
            }
            stat.st_size.set(fileStatus.getLen()); // File size
            stat.st_mtim.tv_sec.set(fileStatus.getModificationTime() / 1000); // Last modified time
            stat.st_nlink.set(1); // Default number of links for files

            return 0; // Success
        } catch (FileNotFoundException e) {
            return -2; // -ENOENT (Not found)
        } catch (IOException e) {
            logger.error("Error getting attributes for path: {}", path, e);
            return -1; // Generic error
        }
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo info) {
        try {
            Path hdfsPath = getHdfsPath(path);

            // Vérification si le répertoire existe et est valide dans HDFS
            if (!hdfs.exists(hdfsPath) || !hdfs.isDirectory(hdfsPath)) {
                return -2; // -ENOENT : Répertoire introuvable
            }

            // Ajout des dossiers par défaut "." et ".."
            filter.apply(buf, ".", null, 0);
            filter.apply(buf, "..", null, 0);

            // Récupération du contenu du répertoire HDFS
            FileStatus[] fileStatuses = hdfs.listStatus(hdfsPath);
            for (FileStatus file : fileStatuses) {
                // Ajout de chaque fichier/répertoire au contenu du répertoire actuel
                String fileName = file.getPath().getName();
                FileStat stat = new FileStat(Runtime.getSystemRuntime()); // Création des métadonnées pour chaque entrée

                if (file.isDirectory()) {
                    stat.st_mode.set(FileStat.S_IFDIR | 0755); // Répertoire avec permissions
                } else {
                    stat.st_mode.set(FileStat.S_IFREG | 0644); // Fichier régulier avec permissions
                }
                stat.st_size.set(file.getLen()); // Taille du fichier
                stat.st_mtim.tv_sec.set(file.getModificationTime() / 1000); // Temps de modification

                // Ajout de l'entrée au répertoire via le filtre
                filter.apply(buf, fileName, stat, 0);
            }

            return 0; // Succès
        } catch (IOException e) {
            logger.error("Erreur lors de la lecture du répertoire pour le chemin : {}", path, e);
            return -1; // Erreur générique
        }
    }

    @Override
    public int read(String path, Pointer buffer, long size, long offset, FuseFileInfo fileInfo) {
        try {
            Path hdfsPath = getHdfsPath(path);

            if (!hdfs.exists(hdfsPath)) {
                return -2; // -ENOENT (Not found)
            }

            // Open the file and read the requested portion
            FSDataInputStream inputStream = hdfs.open(hdfsPath);
            inputStream.seek(offset);

            byte[] bytes = new byte[(int) size];
            int bytesRead = inputStream.read(bytes);
            if (bytesRead > 0) {
                buffer.put(0, bytes, 0, bytesRead);
                return bytesRead; // Number of bytes read
            }
            return 0; // EOF
        } catch (IOException e) {
            logger.error("Error reading file for path: {}", path, e);
            return -1; // Generic error
        }
    }

    @Override
    public int create(String path, long mode, FuseFileInfo fileInfo) {
        try {
            Path hdfsPath = getHdfsPath(path);

            // Create the file in HDFS
            if (hdfs.exists(hdfsPath)) {
                return -17; // -EEXIST (File already exists)
            }
            hdfs.create(new Path(hdfsPath.toString())).close();

            logger.info("File created: {}", hdfsPath);
            return 0; // Success
        } catch (IOException e) {
            logger.error("Error creating file at path: {}", path, e);
            return -1; // Generic error
        }
    }

    @Override
    public int write(String path, Pointer buffer, long size, long offset, FuseFileInfo fileInfo) {
        try {
            Path hdfsPath = getHdfsPath(path);
            byte[] data = new byte[(int) size];
            buffer.get(0, data, 0, (int) size);

            // Write data at the specified offset using HDFS output streams
            FSDataInputStream existingData = null;
            ByteArrayOutputStream resultData = new ByteArrayOutputStream();
            if (hdfs.exists(hdfsPath)) {
                existingData = hdfs.open(hdfsPath);

                // Handle overwriting if offset is specified
                byte[] oldData = new byte[(int) offset];
                existingData.readFully(oldData);
                resultData.write(oldData);
            }

            resultData.write(data);
            hdfs.create(hdfsPath, true).write(resultData.toByteArray());

            logger.info("Wrote {} bytes to file at path: {}", size, hdfsPath);
            return (int) size;
        } catch (IOException e) {
            logger.error("Error writing to file at path: {}", path, e);
            return -1; // Generic error
        }
    }


    @Override
    public int unlink(String path) {
        try {
            Path hdfsPath = getHdfsPath(path);

            if (!hdfs.exists(hdfsPath)) {
                return -2; // -ENOENT (File not found)
            }
            if (!hdfs.delete(hdfsPath, false)) {
                return -1; // Generic error if unable to delete
            }

            logger.info("File deleted: {}", hdfsPath);
            return 0; // Success
        } catch (IOException e) {
            logger.error("Error deleting file at path: {}", path, e);
            return -1; // Generic error
        }
    }

    @Override
    public int mkdir(String path, long mode) {
        try {
            Path hdfsPath = getHdfsPath(path);

            if (hdfs.exists(hdfsPath)) {
                return -17; // -EEXIST (Directory already exists)
            }
            if (!hdfs.mkdirs(hdfsPath)) {
                return -1; // Generic error if unable to create directory
            }

            logger.info("Directory created: {}", hdfsPath);
            return 0; // Success
        } catch (IOException e) {
            logger.error("Error creating directory at path: {}", path, e);
            return -1; // Generic error
        }
    }


    @Override
    public int rmdir(String path) {
        try {
            Path hdfsPath = getHdfsPath(path);

            if (!hdfs.exists(hdfsPath) || !hdfs.isDirectory(hdfsPath)) {
                return -2; // -ENOENT (Directory not found or not a directory)
            }

            FileStatus[] fileStatuses = hdfs.listStatus(hdfsPath);
            if (fileStatuses.length > 0) {
                return -39; // -ENOTEMPTY (Directory not empty)
            }

            if (!hdfs.delete(hdfsPath, false)) {
                return -1; // Generic error if unable to delete
            }

            logger.info("Directory deleted: {}", hdfsPath);
            return 0; // Success
        } catch (IOException e) {
            logger.error("Error deleting directory at path: {}", path, e);
            return -1; // Generic error
        }
    }

    private Path getHdfsPath(String path) {
        // Adjust the path to map local FUSE paths to HDFS paths
        String normalizedPath = path.equals("/") ? "" : path;
        return new Path(rootPath + normalizedPath);
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: MyVirtualFileSystem <HDFS_URI> <HDFS_ROOT_PATH> <LOCAL_MOUNT_POINT>");
            System.exit(1);
        }

        String hdfsUri = args[0];
        String hdfsRootPath = args[1];
        String localMountPoint = args[2];

        HdfsVirtualFileSystem fileSystem = new HdfsVirtualFileSystem(hdfsUri, hdfsRootPath);

        try {
            fileSystem.mount(Paths.get(localMountPoint), true, false); // true -> debug mode
        } finally {
            fileSystem.umount();
        }
    }
}