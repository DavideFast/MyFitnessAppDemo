import express from "express";
import cors from "cors";
import dotenv from "dotenv";

import { createSystemRouter } from "./routes/systemRoutes.js";
import { createServerContext } from "./bootstrap/serverContext.js";
import { createClickhouseClient } from "./db/pool.js";
import { Kafka } from "kafkajs";
import { AppsV1Api, BatchV1Api, CustomObjectsApi, KubeConfig } from "@kubernetes/client-node";

// ============================ CONFIGURAZIONE ===============================

// Configurazione del client Kubernetes
const kubeConfig = new KubeConfig();
kubeConfig.loadFromDefault();
const k8sApi = kubeConfig.makeApiClient(AppsV1Api);
const k8sBatchApi = kubeConfig.makeApiClient(BatchV1Api);
const k8sCustomObjectsApi = kubeConfig.makeApiClient(CustomObjectsApi);
const kubernetesNamespace = process.env.KUBERNETES_NAMESPACE || "bigintensive";
const eltCronJobName = "elt-copy-workout";
const runningPopulationJobName = "running-population-analysis";
const sparkApplicationGroup = "sparkoperator.k8s.io";
const sparkApplicationVersion = "v1beta2";
const sparkApplicationPlural = "sparkapplications";

// Configurazione del produttore Kafka
const kafkaBrokers = String(process.env.KAFKA_BOOTSTRAP_SERVERS || "kafka:19092")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: "my-express-api",
  brokers: kafkaBrokers,
});
const producer = kafka.producer();
const smartwatchKafkaTopic = process.env.SMARTWATCH_KAFKA_TOPIC || "heart-rate-events";
let producerConnected = false;

async function ensureProducerConnected() {
  if (!producerConnected) {
    await producer.connect();
    producerConnected = true;
  }
}

function getKubernetesErrorMessage(error) {
  return error?.body?.message || error?.response?.body?.message || error?.message || "Errore Kubernetes sconosciuto";
}

// ============================ SERVER ===============================
dotenv.config();
const context = createServerContext(import.meta.url);
const app = express();

app.use(
  cors({
    origin(origin, callback) {
      if (context.isAllowedOrigin(origin)) {
        return callback(null, true);
      }

      return callback(new Error(`CORS blocked for origin: ${origin}`));
    },
  }),
);
app.use(express.json());
app.use(createSystemRouter());

// =========================== READ/WRITE DATABASE  ==============================
app.get("/api/v1/readPostgresql", (req, res) => {
  const query = "SELECT * FROM allenamenti WHERE athlete_id = 1 LIMIT 10";
  context.pool.query(query, (err, result) => {
    if (err) {
      console.error("Errore query PostgreSQL:", err.message);
      return res.status(500).json({ success: false, error: err.message });
    }
    console.log("Dati letti da PostgreSQL:", result.rows);
    res.json({ success: true, count: result.rows.length, data: result.rows });
  });
});

app.get("/api/v1/writePostgresql", (req, res) => {
  const allenamento = {
    allenamento: {
      data: "2026-08-15",
      tipo: "Forza - Push Day",
      durata_minuti: 75,
      note: "Ottimo volume sul petto, progresso nel carico rispetto alla settimana scorsa.",
      esercizi: [
        {
          nome: "Panca Piana con bilanciere",
          gruppo_muscolare: "Petto",
          serie: [
            { numero: 1, ripetizioni: 10, carico_kg: 60 },
            { numero: 2, ripetizioni: 8, carico_kg: 65 },
            { numero: 3, ripetizioni: 6, carico_kg: 70 },
          ],
        },
        {
          nome: "Military Press",
          gruppo_muscolare: "Spalle",
          serie: [
            { numero: 1, ripetizioni: 10, carico_kg: 30 },
            { numero: 2, ripetizioni: 10, carico_kg: 30 },
            { numero: 3, ripetizioni: 8, carico_kg: 35 },
          ],
        },
        {
          nome: "Croci ai cavi",
          gruppo_muscolare: "Petto",
          serie: [
            { numero: 1, ripetizioni: 12, carico_kg: 15 },
            { numero: 2, ripetizioni: 12, carico_kg: 15 },
            { numero: 3, ripetizioni: 12, carico_kg: 15 },
          ],
        },
        {
          nome: "Pushdown tricipiti",
          gruppo_muscolare: "Tricipiti",
          serie: [
            { numero: 1, ripetizioni: 15, carico_kg: 20 },
            { numero: 2, ripetizioni: 12, carico_kg: 25 },
            { numero: 3, ripetizioni: 10, carico_kg: 30 },
          ],
        },
      ],
    },
  };
  const query = `INSERT INTO allenamenti (athlete_id, data_allenamento, tipo_allenamento, durata_minuti, struttura_allenamento)
                 VALUES ($1, $2, $3, $4, $5)`;
  const values = [1, "2026-08-15", "forza", 75, JSON.stringify(allenamento)];
  context.pool.query(query, values, (err, result) => {
    if (err) {
      console.error("Errore query PostgreSQL:", err.message);
      return res.status(500).json({ success: false, error: err.message });
    }
    res.json({ success: true });
  });
});

app.get("/api/v1/readClickhouse", async (req, res) => {
  console.log("Leggo da : " + process.env.CLICKHOUSE_HOST + ":" + process.env.CLICKHOUSE_PORT);
  console.log("Leggo da ClickHouse con utente: " + process.env.CLICKHOUSE_USER);
  console.log("Leggo da ClickHouse con database: " + process.env.CLICKHOUSE_DATABASE);
  console.log("Leggo da ClickHouse con url: " + (process.env.CLICKHOUSE_URL ? "****" : "(vuota)"));
  console.log("Leggo da ClickHouse con formato: JSONEachRow");
  const query_grande = "SELECT * FROM bigintensive.running_samples WHERE session_id = ";
  const query_media = "(SELECT session_id FROM bigintensive.running_samples WHERE athlete_id = () LIMIT 1)";
  const query_piccola = "(SELECT athlete_id FROM bigintensive.running_samples LIMIT 1)";
  const query = query_grande + query_media.replace("()", query_piccola);
  try {
    const valore = await createClickhouseClient.query({ query, format: "JSONEachRow" });
    const data = await valore.json();
    console.log("Dati letti da ClickHouse:", data);
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    console.error("Errore query ClickHouse:", error.message);
    res.status(503).json({ success: false, error: "ClickHouse non disponibile" });
  }
});

app.post("/api/v1/pushToKafka", async (req, res) => {
  try {
    await ensureProducerConnected();
    await producer.send({
      topic: smartwatchKafkaTopic,
      messages: [{ key: req.body.athlete_id.toString() + "-" + req.body.session_id.toString(), value: JSON.stringify(req.body) }],
    });
    res.status(202).json({ success: true, message: "Evento inviato a Kafka" });
  } catch (error) {
    console.error("Errore Kafka:", error);
    res.status(500).json({ success: false, error: "Impossibile inviare l'evento a Kafka" });
  }
});

app.post("/api/v1/startSmartWatchPodSimulator", async (req, res) => {
  try {
    const patch = [{ op: "replace", path: "/spec/replicas", value: 1 }];
    await k8sApi.patchNamespacedDeployment({ name: "smartwatch-simulator", namespace: kubernetesNamespace, body: patch });
    res.status(200).json({ success: true, message: "Simulatore SmartWatch Pod avviato" });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore avviando il simulatore SmartWatch Pod:", message);
    res.status(500).json({ success: false, error: message });
  }
});

app.post("/api/v1/stopSmartWatchPodSimulator", async (req, res) => {
  try {
    const patch = [{ op: "replace", path: "/spec/replicas", value: 0 }];
    await k8sApi.patchNamespacedDeployment({
      name: "smartwatch-simulator",
      namespace: kubernetesNamespace,
      body: patch,
    });
    res.status(200).json({
      success: true,
      message: "Simulatore SmartWatch Pod fermato",
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore fermando il simulatore SmartWatch Pod:", message);
    res.status(500).json({
      success: false,
      error: message,
    });
  }
});

app.post("/api/v1/startELTProcess", async (req, res) => {
  try {
    const cronJob = await k8sBatchApi.readNamespacedCronJob({
      name: eltCronJobName,
      namespace: kubernetesNamespace,
    });

    await k8sBatchApi.patchNamespacedCronJob({
      name: eltCronJobName,
      namespace: kubernetesNamespace,
      body: [{ op: "add", path: "/spec/suspend", value: false }],
    });

    // I job manuali bypassano concurrencyPolicy del CronJob: evitiamo run sovrapposti.
    const existingJobs = await k8sBatchApi.listNamespacedJob({
      namespace: kubernetesNamespace,
      labelSelector: `app=${eltCronJobName}`,
    });

    const runningJob = existingJobs.items.find((job) => (job.status?.active || 0) > 0);

    if (runningJob) {
      return res.status(200).json({
        success: true,
        message: `Processo ELT gia' in esecuzione (${runningJob.metadata.name})`,
      });
    }

    await k8sBatchApi.createNamespacedJob({
      namespace: kubernetesNamespace,
      body: {
        apiVersion: "batch/v1",
        kind: "Job",
        metadata: {
          generateName: `${eltCronJobName}-manual-`,
          labels: cronJob.metadata?.labels,
        },
        spec: {
          ...cronJob.spec.jobTemplate.spec,
          // I job con generateName non rientrano negli history limit del CronJob.
          ttlSecondsAfterFinished: 1800,
        },
      },
    });

    res.status(200).json({
      success: true,
      message: "Processo ELT avviato immediatamente",
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore avviando il processo ELT:", message);
    res.status(500).json({
      success: false,
      error: message,
    });
  }
});

app.post("/api/v1/stopELTProcess", async (req, res) => {
  try {
    await k8sBatchApi.patchNamespacedCronJob({
      name: eltCronJobName,
      namespace: kubernetesNamespace,
      body: [{ op: "add", path: "/spec/suspend", value: true }],
    });
    await k8sBatchApi.deleteCollectionNamespacedJob({
      namespace: kubernetesNamespace,
      labelSelector: "app=elt-copy-workout",
      propagationPolicy: "Background",
    });
    res.status(200).json({
      success: true,
      message: "Processo ELT fermato",
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore fermando il processo ELT:", message);
    res.status(500).json({
      success: false,
      error: message,
    });
  }
});

app.post("/api/v1/startRunningPopulation", async (req, res) => {
  try {
    const existingApplications = await k8sCustomObjectsApi.listNamespacedCustomObject({
      group: sparkApplicationGroup,
      version: sparkApplicationVersion,
      namespace: kubernetesNamespace,
      plural: sparkApplicationPlural,
      labelSelector: `app=${runningPopulationJobName}`,
    });

    const applications = existingApplications.items || existingApplications.body?.items || [];
    const finishedStates = ["COMPLETED", "FAILED", "FAILED_SUBMISSION", "SUBMISSION_FAILED", "UNKNOWN"];
    const activeStates = ["NEW", "SUBMITTED", "RUNNING", "PENDING_RERUN", "RESTARTING", "FAILING"];
    const finishedApplications = applications.filter((application) => {
      const state = application.status?.applicationState?.state;
      return finishedStates.includes(state);
    });
    await Promise.all(
      finishedApplications.map((application) =>
        k8sCustomObjectsApi.deleteNamespacedCustomObject({
          group: sparkApplicationGroup,
          version: sparkApplicationVersion,
          namespace: kubernetesNamespace,
          plural: sparkApplicationPlural,
          name: application.metadata.name,
        }),
      ),
    );

    const runningApplication = applications.find((application) => {
      const state = application.status?.applicationState?.state;
      return !state || activeStates.includes(state);
    });
    if (runningApplication) {
      return res.status(200).json({
        success: true,
        applicationName: runningApplication.metadata.name,
        message: `RunningPopulation gia' in esecuzione (${runningApplication.metadata.name})`,
      });
    }

    const application = await k8sCustomObjectsApi.createNamespacedCustomObject({
      group: sparkApplicationGroup,
      version: sparkApplicationVersion,
      namespace: kubernetesNamespace,
      plural: sparkApplicationPlural,
      body: {
        apiVersion: `${sparkApplicationGroup}/${sparkApplicationVersion}`,
        kind: "SparkApplication",
        metadata: {
          generateName: `${runningPopulationJobName}-`,
          labels: {
            app: runningPopulationJobName,
          },
        },
        spec: {
          type: "Python",
          mode: "cluster",
          image: "davidefast/bigintensive-sparkwithdependencies:latest",
          imagePullPolicy: "Always",
          sparkVersion: "3.5.3",
          mainApplicationFile: "local:///opt/jobs/RunningPopolationAnalysis.py",
          pythonVersion: "3",
          restartPolicy: { type: "Never" },
          sparkConf: {
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            "spark.dynamicAllocation.initialExecutors": "1",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "4",
            "spark.dynamicAllocation.executorIdleTimeout": "60s",
            "spark.dynamicAllocation.cachedExecutorIdleTimeout": "120s",
            "spark.executor.cores": "2",
            "spark.executor.memory": "2g",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.extraClassPath": "/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar",
          },
          driver: {
            cores: 2,
            coreLimit: "2000m",
            memory: "2g",
            serviceAccount: "spark",
            envFrom: [{ configMapRef: { name: "bigintensive-config" } }, { secretRef: { name: "bigintensive-secrets" } }],
          },
          executor: {
            cores: 2,
            memory: "2g",
            envFrom: [{ configMapRef: { name: "bigintensive-config" } }, { secretRef: { name: "bigintensive-secrets" } }],
          },
        },
      },
    });

    res.status(200).json({
      success: true,
      applicationName: application.metadata?.name || application.body?.metadata?.name,
      message: "SparkApplication RunningPopulation avviata",
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore avviando lo Spark job:", message);
    res.status(500).json({
      success: false,
      error: message,
    });
  }
});

// ============================ START SERVER ===============================

async function start() {
  app.listen(context.port, () => {
    console.log(`Backend API listening on http://localhost:${context.port}`);
  });
}

start().catch(console.error);

process.on("SIGINT", async () => {
  if (producerConnected) {
    await producer.disconnect();
  }
  process.exit(0);
});

process.on("SIGTERM", async () => {
  if (producerConnected) {
    await producer.disconnect();
  }
  process.exit(0);
});
