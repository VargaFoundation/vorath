# Test du Plugin CSI Vorath (HDFS) sur Kubernetes

Ce document explique comment déployer et tester le plugin CSI Vorath dans un cluster Kubernetes.

## Prérequis

- Un cluster Kubernetes fonctionnel.
- Un cluster HDFS accessible (avec ou sans Kerberos).
- `kubectl` configuré pour accéder au cluster.
- FUSE installé sur les nœuds Kubernetes (pour le montage HDFS).

## Configuration du Secret HDFS

Le plugin a besoin des fichiers de configuration HDFS et, si Kerberos est activé, d'un principal et d'un fichier keytab. Ces informations sont stockées dans un Secret Kubernetes.

### Création du Secret

Préparez vos fichiers `core-site.xml`, `hdfs-site.xml` et votre `user.keytab`.

```bash
kubectl create secret generic hdfs-csi-config \
  --from-literal=principal=user/host@REALM \
  --from-file=keytab=user.keytab \
  --from-file=core-site.xml=core-site.xml \
  --from-file=hdfs-site.xml=hdfs-site.xml
```

*Note : Si vous n'utilisez pas Kerberos, vous pouvez mettre des valeurs fictives pour `principal` et `keytab`, mais les clés doivent être présentes dans le Secret.*

## Déploiement du Plugin

Utilisez les templates fournis dans le dossier `kubernetes/templates`.

```bash
kubectl apply -f kubernetes/templates/
```

Assurez-vous que les Pods `vorath-controller` et `vorath-node` sont en cours d'exécution.

## Test de Provisionnement Statique

### 1. Création d'un PersistentVolume (PV)

Créez un fichier `pv-hdfs.yaml` :

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: hdfs-pv
spec:
  capacity:
    storage: 5Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  csi:
    driver: varga.foundation.vorath
    volumeHandle: hdfs-volume-1
    volumeAttributes:
      location: "hdfs://namenode:8020/volumes/vol1"
      secretName: "hdfs-csi-config"
      secretNamespace: "default"
```

### 2. Création d'un PersistentVolumeClaim (PVC)

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: hdfs-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  volumeName: hdfs-pv
```

### 3. Utilisation dans un Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: hdfs-test-pod
spec:
  containers:
  - name: web-server
    image: nginx
    volumeMounts:
    - name: hdfs-storage
      mountPath: /data
  volumes:
  - name: hdfs-storage
    persistentVolumeClaim:
      claimName: hdfs-pvc
```

## Vérification

Une fois le Pod démarré, vous pouvez vérifier que le montage HDFS est effectif :

```bash
kubectl exec hdfs-test-pod -- ls /data
kubectl exec hdfs-test-pod -- touch /data/testfile
```

Vérifiez également les logs des nœuds CSI en cas de problème :
```bash
kubectl logs -l app=vorath-node -c vorath-plugin
```

## Dépannage Common

- **Erreur de montage FUSE** : Vérifiez que les nœuds ont `/dev/fuse` et que le conteneur a les privilèges nécessaires (`privileged: true`).
- **Erreur Kerberos** : Vérifiez l'heure des nœuds (doit être synchronisée) et la validité du keytab.
- **Socket Unix** : Le plugin CSI communique via un socket Unix partagé. Vérifiez les `volumeMounts` pour `/var/lib/kubelet/plugins`.
