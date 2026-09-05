# MyFitnessHub deployment on k3s

This repository contains the Kubernetes manifests and instructions to deploy MyFitnessHub on a k3s cluster.
This folder contains the Kubernetes base to run MyFitnessHub on k3s without using Docker Compose.

For technical details of the manifests (ownership, complete order, change checklist), see [MANIFESTS.md](MANIFESTS.md).

The cluster manages:

- `backend`
- `frontend`
- `postgres`
- `kafka`
- `kafka-ui`

## Index:

- [Topologia distribuita prevista](#topologia-distribuita-prevista)
- [Creazione della macchina virtuale con Ubuntu](#creazione-della-macchina-virtuale-con-ubuntu-altri-sistemi-operativi-non-sono-testati)
- [Creazione rete LAN del cluster](#creazione-rete-lan-del-cluster)
- [Step 0: verifica il cluster](#step-0-verifica-il-cluster)
- [Step 1: configurazione master](#step-1-configurazione-master)
- [Step 2: configurazione worker](#step-2-configurazione-worker)
- [Step 3: applica i manifest](#step-3-applica-i-manifest)
- [Step 4: verifica stabilità del cluster](#step-4-verifica-stabilit%C3%A0-del-cluster)
- [Troubleshooting rapido](#troubleshooting-rapido)

# Expected Distributed Topology

The target cluster uses 3 physical PCs, each registered as a K3s node:

```text
PC 1: K3s server/control-plane + workload
PC 2: K3s agent/worker + workload
PC 3: K3s agent/worker + workload
```

The expected composition of distributed services is:

```text
Kafka
  3 broker/controller in modalità KRaft, uno per nodo
  replication factor dei topic: 3
  min.insync.replicas: 2
  KRaft coordina Kafka; non viene usato ZooKeeper

ClickHouse
  4 pod server: 2 shard x 2 repliche
  ClickHouse Keeper: 3 pod, uno per nodo
  tabelle locali ReplicatedMergeTree
  tabelle Distributed per l'accesso all'intero cluster

Spark
  modalità native Kubernetes
  Jupyter può funzionare come driver per le dimostrazioni
  gli executor vengono creati da Spark su richiesta
  K3s distribuisce gli executor sui nodi disponibili
```

Note: the bootstrap of Kafka topics creates new topics with replication factor `3`, but
does not modify existing topics created with replication factor `1`. On an already
populated cluster, a reassignment migration is required; in development, you can use
`RESET_NAMESPACE=true` only if data deletion is acceptable.

KRaft and ClickHouse Keeper are distinct components: KRaft coordinates Kafka, while Keeper coordinates ClickHouse replicas. Adding a fourth PC does not require a new fixed role for Spark; K3s can schedule new executors on available nodes when a job requires more.

<br>
<br>

# Creation of the virtual machine with Ubuntu (other operating systems are not tested).

The information for creating the virtual machine refers to Hyper-V and VirtualBox.
In Hyper-V, create a new virtual machine with the following settings:

- Generation 2
- Memory: 16000 MB
- Hard disk: 500 GB
- External network card: Default Switch (Wi-fi)
- Intra-cluster network card: External Virtual Switch (connected to the physical network)
- Disable Secure Boot
- ISO Image: Ubuntu 25.04/26.04 LTS

The order of insertion of the network card influences which interface will be used for that network. The first inserted will be called eth0 while the second eth1. The intra-cluster network card must be configured to enable MAC address spoofing.

In VirtualBox, create a new virtual machine with the following settings:

- Memory: 16000 MB
- Cores: 5
- Hard disk: 500 GB
- Intra-cluster network card: Bridge (connected to the physical network)
- External network card: NAT
- ISO Image: Ubuntu 25.04/26.04 LTS

The order of insertion of the network card influences which interface will be used for that network. The first inserted will be called enp0s3 while the second enp0s8. You need to check "allow all" on the virtual switch to enable MAC address spoofing.

<br>
<br>

# Creation of the cluster LAN

The project is designed to run on 3 physical nodes. Therefore, the topology and configurations are optimized for this scenario.
The K3s server node hosts the cluster control and can also run workloads. The agent/worker nodes only host workloads. Kafka and ClickHouse services are distributed across all nodes to ensure resilience and performance.

The system is interconnected via a network switch. All nodes are connected via ethernet cable to the switch (without management)
and therefore must be configured with static IPs. The nodes will have another network card (being Wi-Fi laptops) that they will use to access the internet for updates and package downloads. As well as to simulate remote access to the cluster from an external PC. In this case, the K3s server node must be reachable from the outside via its static IP.

```text
sudo ip addr add <IP_STATIC_NODE>/24 dev <INTERFACE>
```

To see the available network interfaces:

```bash
ip addr show
```

Only for VirtualBox

```bash
sudo nmcli device set enp0s3 managed no
```

```bash
# IP configuration of the master
sudo ip addr add 192.168.1.10/24 dev eth0
```

```bash
# IP configuration of worker-1
sudo ip addr add 192.168.1.20/24 dev eth1
```

```bash
# IP configuration of worker-2
sudo ip addr add 192.168.1.30/24 dev enp0s3
```

Then test the connectivity between the nodes with ping:

```text
ping <IP_STATIC_NODE>
```

If you want these configurations to persist, you need to use the nmcli commands:

```bash
sudo nmcli device status
sudo nmcli connection down <NOME>
sudo nmcli connection delete <NOME>
sudo nmcli connection add type ethernet con-name <NOME> ifname <INTERFACE> ip4 <IP_STATIC_NODO>/24
sudo nmcli connection up <NOME>
sudo nmcli connection show

```

<br>
<br>

# Step 0: verify the cluster

Make sure that k3s is running and that `kubectl` is pointing to its context:

```powershell
kubectl config current-context
kubectl get nodes
```

If the context is not correct, select it before continuing.

<br>
<br>

# Step 1: master configuration

## Preliminary update of the operating system:

```bash
sudo apt update && sudo apt upgrade -y
```

## Install basic services

```bash
sudo apt install git ca-certificates helm -y
```

Helm is necessary to install the Spark Operator, which manages Spark jobs on Kubernetes. Helm is a package manager like apt or npm that allows you to quickly install complex applications like the Spark Operator (requires coordination of multiple yaml files).

## Install K3s

```bash
curl -sfL https://get.k3s.io | sh -
```

## Copy the project repository to the user's home directory

```bash
cd ~
git clone https://github.com/DavideFast/BigIntensive.git
```

## Configure the K3s master node

Create the config.yaml file for the k3s server node:

```bash
sudo mkdir -p /etc/rancher/k3s
sudo nano /etc/rancher/k3s/config.yaml
```

Insert the following content, replacing `<IP_STATIC_NODE>` with the static IP of the server node and `<INTERFACE>` with the correct network interface:

```yaml
node-ip: "<IP_STATIC_NODE>"
flannel-iface: "<INTERFACE>"
```

Restart the k3s-server service:

```bash
sudo systemctl daemon-reload
sudo systemctl restart k3s
```

## Retrieve the token for joining agent/worker nodes

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

In this specific case:

```bash
#TOKEN
K10f8eed66f2b617a72eb273c752e47b1e9f81f0132219beec50ec42101b25d6dd0::server:926a3320a8d2afe48113eb997039ce7c
```

<br>
<br>

# Step 2: worker configuration

The virtual machine must be configured like the master server, with the same memory, disk, and network settings.

## Install K3s as an agent/worker:

Initialize the node with the command:

```bash
sudo apt update && sudo apt install curl -y
```

Install K3s as an agent/worker, replacing `<NOME_ASSEGNATO>` with a unique name for the node and `<SERVER_IP>` and `<TOKEN>` with the values obtained from the master server:

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="agent --node-name <NOME_ASSEGNATO>" K3S_URL=https://<SERVER_IP>:6443 K3S_TOKEN='<TOKEN>' sh -
```

You need to update the node configuration file:

```bash
sudo mkdir -p /etc/rancher/k3s
sudo nano /etc/rancher/k3s/config.yaml
```

Inside the file write:

```yaml
server: "https://<SERVER_IP>:6443"
token: "<TOKEN>"
node-ip: "<IP_STATIC_NODE>"
flannel-iface: "<INTERFACE>"
```

After that, save the file and restart the k3s-agent service:

```bash
sudo systemctl daemon-reload
sudo systemctl restart k3s-agent
```

> [!TIP]
> You may need to open some ports on the master firewall, exposing:
>
> - 6443 TCP per l'API server di Kubernetes (worker->master)
> - 8472 UDP per il traffico di rete tra i nodi (flannel VXLAN)
> - 10250 TCP per il kubelet (master->worker e traffico interno)
>
> The rules should be added directly via iptables and not through ufw, which is not active by default on Ubuntu Desktop.

<br>
<br>

# Step 3: apply the manifests

If this is the first installation, you can deploy all the manifests with:

```bash
bash k3s/deploy-all.sh
```

Otherwise, if it is already active and there is no saved data, you can deploy with:

```bash
RESET_NAMESPACE=true bash k3s/deploy-all.sh
```

<br>
<br>

# Step 4: check cluster stability

## Check that the pods are active and stable

```bash
kubectl get pods -n bigintensive -w
```

Initially expect `ContainerCreating` on the stateful services, then `Running` for the application pods.

## Check PostgreSQL

The `postgres` StatefulSet mounts `database/postgresql/schema.sql` as an initialization script. The script is executed by PostgreSQL only on the first start of the data volume.

```bash
kubectl get statefulset postgres -n bigintensive
kubectl logs statefulset/postgres -n bigintensive
```

## Expose the services in the browser

The manifest uses `traefik` as the ingress class and these hosts:

- `http://bigintensive.local` for the frontend
- `http://api.bigintensive.local` for the backend
- `http://kafka-ui.bigintensive.local` for Kafka UI
- `http://jupyter.bigintensive.local` for Jupyter

You need to point these names to the IP of the k3s node in the hosts file of the machine you are browsing from.

If you are browsing from both host machines, update the hosts file on both with the IP of the server VM.

```bash
sudo nano /etc/hosts
```

Example hosts file:

```text
192.168.1.10 bigintensive.local
192.168.1.10 api.bigintensive.local
192.168.1.10 kafka-ui.bigintensive.local
192.168.1.10 jupyter.bigintensive.local
```

Opening `http://192.168.1.10` directly is not enough, because the Ingress routes based on the requested hostname.

## Check the app

For a quick automatic check of deploy status, rollout, and services:

```bash
bash k3s/validate-deploy.sh
```

If you want to do deploy and checks in a single command:

```bash
bash k3s/validate-deploy.sh --deploy-first
```

First check the health of the backend:

```bash
kubectl port-forward -n bigintensive svc/backend 3001:3001
```

Then open:

- `http://localhost:3001/health`
- `http://bigintensive.local`
- `http://api.bigintensive.local/health`
- `http://kafka-ui.bigintensive.local`
- `http://jupyter.bigintensive.local`

<br>
<br>

For advanced operations (selective component deployment, extended runtime checks, change checklist), see [MANIFESTS.md](MANIFESTS.md).

# Throubleshooting

It may happen that the virtual switch used by Hyper-V could become misconfigured or disconnected, causing network issues for the Kubernetes cluster. In such cases this command could help to reset the virtual switch:

```powershell
Get-CimInstance Win32_Service -Filter "Name='SharedAccess'" | Select-Object -ExpandProperty ProcessId
Stop-Process -Id <PID> -Force
Restart-Service vmms -Force
```

Check if ehtX (the one attached to the default switch in Hyper-V) has a valid IP address assigned by the DHCP server.
