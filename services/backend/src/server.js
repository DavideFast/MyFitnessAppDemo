import express from "express";
import cors from "cors";
import dotenv from "dotenv";

import { createSystemRouter } from "./routes/systemRoutes.js";
import { createServerContext } from "./bootstrap/serverContext.js";
import { createClickhouseClient } from "./db/pool.js";
import { Kafka } from "kafkajs";
import { AppsV1Api, CoreV1Api, CustomObjectsApi, KubeConfig } from "@kubernetes/client-node";

// ============================ CONFIGURAZIONE ===============================

// Configurazione del client Kubernetes
const kubeConfig = new KubeConfig();
kubeConfig.loadFromDefault();
const k8sApi = kubeConfig.makeApiClient(AppsV1Api);
const k8sCoreApi = kubeConfig.makeApiClient(CoreV1Api);
const k8sCustomObjectsApi = kubeConfig.makeApiClient(CustomObjectsApi);
const kubernetesNamespace = process.env.KUBERNETES_NAMESPACE || "bigintensive";
const eltCronJobName = "elt-copy-workout";
const eltControlConfigMapName = "elt-control";
const argoGroup = "argoproj.io";
const argoVersion = "v1alpha1";
const argoWorkflowsPlural = "workflows";
const eltArgoWorkflowTemplateName = process.env.ELT_ARGO_WORKFLOW_TEMPLATE || "elt-pipeline-template";
const runningPopulationJobName = "running-population-analysis";
const exerciseCorrelationJobName = "exercise-correlation-analysis";
const sparkApplicationGroup = "sparkoperator.k8s.io";
const sparkApplicationVersion = "v1beta2";
const sparkApplicationPlural = "sparkapplications";
const sparkJobsImage = process.env.SPARK_JOBS_IMAGE || "davidefast/bigintensive-sparkwithdependencies:latest";
const sparkFinishedStates = ["COMPLETED", "FAILED", "FAILED_SUBMISSION", "SUBMISSION_FAILED", "UNKNOWN"];
const sparkActiveStates = ["NEW", "SUBMITTED", "RUNNING", "PENDING_RERUN", "RESTARTING", "FAILING"];
const sparkJdbcExtraClassPath = "/opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar:/opt/spark/jars/postgresql-42.7.2.jar";
const sparkApplicationConf = {
  "spark.dynamicAllocation.enabled": "true",
  "spark.dynamicAllocation.shuffleTracking.enabled": "true",
  "spark.dynamicAllocation.initialExecutors": "1",
  "spark.dynamicAllocation.minExecutors": "1",
  "spark.dynamicAllocation.maxExecutors": "4",
  "spark.dynamicAllocation.executorIdleTimeout": "60s",
  "spark.dynamicAllocation.cachedExecutorIdleTimeout": "120s",
  "spark.executor.cores": "2",
  "spark.executor.memory": "4g",
  "spark.sql.shuffle.partitions": "10",
  "spark.driver.extraClassPath": sparkJdbcExtraClassPath,
  "spark.executor.extraClassPath": sparkJdbcExtraClassPath,
};
const sparkPodEnvFrom = [{ configMapRef: { name: "bigintensive-config" } }, { secretRef: { name: "bigintensive-secrets" } }];
const sparkDriverSpec = {
  cores: 2,
  coreLimit: "2000m",
  memory: "4g",
  serviceAccount: "spark",
  envFrom: sparkPodEnvFrom,
};
const sparkExecutorSpec = {
  cores: 2,
  memory: "4g",
  envFrom: sparkPodEnvFrom,
};

const sparkJobDefinitions = {
  runningPopulation: {
    key: "runningPopulation",
    jobName: runningPopulationJobName,
    displayName: "RunningPopulation",
    mainApplicationFile: process.env.RUNNING_POPULATION_MAIN_FILE || "local:///opt/jobs/RunningPopolationAnalysis.py",
  },
  exerciseCorrelation: {
    key: "exerciseCorrelation",
    jobName: exerciseCorrelationJobName,
    displayName: "ExerciseCorrelation",
    mainApplicationFile: process.env.EXERCISE_CORRELATION_MAIN_FILE || "local:///opt/jobs/ExerciseCorrelation.py",
  },
};

function getSparkJobDefinition(job) {
  const normalizedJob = String(job || "")
    .trim()
    .toLowerCase();
  const aliases = {
    runningpopulation: "runningPopulation",
    "running-population": "runningPopulation",
    running_population: "runningPopulation",
    exercisecorrelation: "exerciseCorrelation",
    "exercise-correlation": "exerciseCorrelation",
    exercise_correlation: "exerciseCorrelation",
  };

  return sparkJobDefinitions[aliases[normalizedJob]] || null;
}

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

function getKubernetesErrorDetails(error) {
  const body = error?.body || error?.response?.body;
  return {
    statusCode: Number(error?.statusCode || error?.status || error?.response?.statusCode || body?.code || 0) || 0,
    reason: body?.reason || error?.name || "Unknown",
    message: body?.message || error?.message || "Errore Kubernetes sconosciuto",
    group: body?.details?.group || null,
    resource: body?.details?.kind || body?.details?.resource || null,
    name: body?.details?.name || null,
  };
}

function isKubernetesForbiddenError(error) {
  const statusCode = error?.statusCode || error?.status || error?.response?.statusCode || error?.body?.code;
  return Number(statusCode) === 403;
}

function createRequestId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function toInteger(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.trunc(parsed);
}

async function setEtlStopAfterWindow(stopAfterWindow) {
  await k8sCoreApi.patchNamespacedConfigMap({
    name: eltControlConfigMapName,
    namespace: kubernetesNamespace,
    body: [{ op: "add", path: "/data/stop-after-window", value: String(stopAfterWindow) }],
  });
}

async function submitEtlArgoWorkflow({ requestedReplicas, requestId } = {}) {
  const workflowLabels = {
    app: eltCronJobName,
    "elt-orchestrator": "argo",
  };

  const parameters = [];
  if (requestedReplicas !== undefined && requestedReplicas !== null && requestedReplicas !== "") {
    const replicas = Math.max(1, toInteger(requestedReplicas, 1));
    parameters.push({ name: "requested-replicas", value: String(replicas) });
  }

  const workflowBody = {
    apiVersion: `${argoGroup}/${argoVersion}`,
    kind: "Workflow",
    metadata: {
      generateName: "elt-argo-manual-",
      namespace: kubernetesNamespace,
      labels: workflowLabels,
    },
    spec: {
      workflowTemplateRef: {
        name: eltArgoWorkflowTemplateName,
      },
      arguments: {
        parameters,
      },
    },
  };

  console.log(`[ELT-ARGO][${requestId}] create workflow: template=${eltArgoWorkflowTemplateName}, requestedReplicas=${parameters[0]?.value || "auto"}`);

  const created = await k8sCustomObjectsApi.createNamespacedCustomObject({
    group: argoGroup,
    version: argoVersion,
    namespace: kubernetesNamespace,
    plural: argoWorkflowsPlural,
    body: workflowBody,
  });

  return created?.metadata?.name || created?.body?.metadata?.name;
}

async function getActiveArgoEtlWorkflow({ requestId } = {}) {
  try {
    console.log(`[ELT-ARGO][${requestId}] checking active workflows in namespace=${kubernetesNamespace}`);
    const response = await k8sCustomObjectsApi.listNamespacedCustomObject({
      group: argoGroup,
      version: argoVersion,
      namespace: kubernetesNamespace,
      plural: argoWorkflowsPlural,
      labelSelector: "app=elt-copy-workout,elt-orchestrator=argo",
    });

    const items = response?.items || response?.body?.items || [];
    const active = items.find((workflow) => {
      const phase = workflow?.status?.phase;
      return phase !== "Succeeded" && phase !== "Failed" && phase !== "Error";
    });
    console.log(`[ELT-ARGO][${requestId}] workflows found=${items.length}, active=${active?.metadata?.name || "none"}`);
    return active;
  } catch (error) {
    if (isKubernetesForbiddenError(error)) {
      console.warn(`[ELT-ARGO][${requestId}] RBAC: list workflows non consentito al backend-controller. Salto il controllo workflow attivo e provo la creazione.`);
      return null;
    }
    throw error;
  }
}

async function startSparkApplication(jobDefinition) {
  const existingApplications = await k8sCustomObjectsApi.listNamespacedCustomObject({
    group: sparkApplicationGroup,
    version: sparkApplicationVersion,
    namespace: kubernetesNamespace,
    plural: sparkApplicationPlural,
    labelSelector: `app=${jobDefinition.jobName}`,
  });

  const applications = existingApplications.items || existingApplications.body?.items || [];

  const finishedApplications = applications.filter((application) => {
    const state = application.status?.applicationState?.state;
    return sparkFinishedStates.includes(state);
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
    return !state || sparkActiveStates.includes(state);
  });

  if (runningApplication) {
    return {
      alreadyRunning: true,
      applicationName: runningApplication.metadata.name,
      message: `${jobDefinition.displayName} gia' in esecuzione (${runningApplication.metadata.name})`,
    };
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
        generateName: `${jobDefinition.jobName}-`,
        labels: {
          app: jobDefinition.jobName,
          sparkJob: jobDefinition.key,
        },
      },
      spec: {
        type: "Python",
        mode: "cluster",
        image: sparkJobsImage,
        imagePullPolicy: "Always",
        sparkVersion: "3.5.3",
        mainApplicationFile: jobDefinition.mainApplicationFile,
        pythonVersion: "3",
        restartPolicy: { type: "Never" },
        sparkConf: sparkApplicationConf,
        driver: sparkDriverSpec,
        executor: sparkExecutorSpec,
      },
    },
  });

  return {
    alreadyRunning: false,
    applicationName: application.metadata?.name || application.body?.metadata?.name,
    message: `SparkApplication ${jobDefinition.displayName} avviata`,
  };
}

async function handleStartSparkJob(req, res, forcedJob) {
  try {
    const jobDefinition = getSparkJobDefinition(forcedJob || req.body?.job || req.query?.job);

    if (!jobDefinition) {
      return res.status(400).json({
        success: false,
        error: "Job Spark non valido",
        allowedJobs: Object.keys(sparkJobDefinitions),
      });
    }

    const result = await startSparkApplication(jobDefinition);

    return res.status(200).json({
      success: true,
      job: jobDefinition.key,
      applicationName: result.applicationName,
      alreadyRunning: result.alreadyRunning,
      message: result.message,
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore avviando lo Spark job:", message);
    return res.status(500).json({
      success: false,
      error: message,
    });
  }
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

app.post("/api/v1/startELTArgoProcess", async (req, res) => {
  const requestId = createRequestId();
  try {
    console.log(`[ELT-ARGO][${requestId}] request received: replicas=${req.body?.replicas ?? "auto"}`);
    await setEtlStopAfterWindow(false);
    const activeWorkflow = await getActiveArgoEtlWorkflow({ requestId });
    if (activeWorkflow) {
      console.log(`[ELT-ARGO][${requestId}] already running: ${activeWorkflow.metadata?.name}`);
      return res.status(200).json({
        success: true,
        requestId,
        workflowName: activeWorkflow.metadata?.name,
        message: `Workflow Argo ELT gia' in esecuzione (${activeWorkflow.metadata?.name}).`,
      });
    }

    const workflowName = await submitEtlArgoWorkflow({ requestedReplicas: req.body?.replicas, requestId });
    console.log(`[ELT-ARGO][${requestId}] workflow created: ${workflowName || "unknown-name"}`);
    res.status(200).json({
      success: true,
      requestId,
      workflowName,
      templateName: eltArgoWorkflowTemplateName,
      message: "Workflow Argo ELT avviato.",
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    const details = getKubernetesErrorDetails(error);
    console.error(`[ELT-ARGO][${requestId}] errore avviando workflow Argo ELT:`, message, details);
    const errorWithHint = isKubernetesForbiddenError(error) ? `${message} | Verifica RBAC backend: kubectl apply -f k3s/03-backend.yaml` : message;
    res.status(500).json({
      success: false,
      requestId,
      error: errorWithHint,
      debug: details,
    });
  }
});

app.post("/api/v1/stopELTAfterWindow", async (req, res) => {
  try {
    await setEtlStopAfterWindow(true);
    res.status(200).json({
      success: true,
      message: "Arresto ELT richiesto: il workflow conclude la finestra corrente e non avvia la successiva.",
    });
  } catch (error) {
    const message = getKubernetesErrorMessage(error);
    console.error("Errore richiedendo l'arresto ELT:", message);
    res.status(500).json({ success: false, error: message });
  }
});

app.post("/api/v1/startSparkJob", async (req, res) => {
  await handleStartSparkJob(req, res);
});

app.post("/api/v1/startRunningPopulation", async (req, res) => {
  await handleStartSparkJob(req, res, "runningPopulation");
});

app.post("/api/v1/startExerciseCorrelation", async (req, res) => {
  await handleStartSparkJob(req, res, "exerciseCorrelation");
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
