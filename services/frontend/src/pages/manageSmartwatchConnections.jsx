export default function ManageSmartwatchConnections() {
  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL || "";

  const avviaSimulazione = () => {
    fetch(`${apiBaseUrl}/api/v1/startSmartWatchPodSimulator`, { method: "POST" })
      .then((response) => response.json())
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error}`);
        }
        console.log("Simulazione avviata:", data);
        alert("Simulazione avviata. Controlla la console per i dettagli.");
      })
      .catch((err) => {
        console.error("Errore nell'avvio della simulazione:", err);
        alert(`Errore nell'avvio della simulazione: ${err.message}`);
      });
  };

  const fermaSimulazione = () => {
    fetch(`${apiBaseUrl}/api/v1/stopSmartWatchPodSimulator`, { method: "POST" })
      .then((response) => response.json())
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error}`);
        }
        console.log("Simulazione fermata:", data);
        alert("Simulazione fermata. Controlla la console per i dettagli.");
      })
      .catch((err) => {
        console.error("Errore nel fermare la simulazione:", err);
        alert(`Errore nel fermare la simulazione: ${err.message}`);
      });
  };

  const avviaELTArgo = () => {
    fetch(`${apiBaseUrl}/api/v1/startELTArgoProcess`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    })
      .then((response) => response.json())
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error || data.message}`);
        }
        console.log("ELT Argo avviato:", data);
        alert(data.message || "Workflow Argo ELT avviato.");
      })
      .catch((err) => {
        console.error("Errore nell'avvio ELT Argo:", err);
        alert(`Errore nell'avvio ELT Argo: ${err.message}`);
      });
  };

  const fermaELTDopoFinestra = () => {
    fetch(`${apiBaseUrl}/api/v1/stopELTAfterWindow`, { method: "POST" })
      .then((response) => response.json())
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error || data.message}`);
        }
        alert(data.message);
      })
      .catch((err) => {
        console.error("Errore nella richiesta di arresto ELT:", err);
        alert(`Errore nell'arresto ELT: ${err.message}`);
      });
  };

  return (
    <section aria-label="Dati workouts">
      <h1>Gestione connessioni Smartwatch </h1>
      <button onClick={avviaSimulazione}>Avvia simulazione</button>
      <button onClick={fermaSimulazione}>Ferma simulazione</button>

      <br />
      <br />
      <h1>Gestione processo ELT (Argo)</h1>
      <button onClick={avviaELTArgo}>Avvia processo ELT (Argo)</button>
      <button onClick={fermaELTDopoFinestra}>Ferma ELT dopo finestra corrente</button>

      <br />
      <br />
      <h1>Gestione job Spark</h1>
      <button
        onClick={() =>
          fetch(`${apiBaseUrl}/api/v1/startRunningPopulation`, { method: "POST" })
            .then((response) => response.json())
            .then((data) => {
              if (!data.success) {
                throw new Error(data.error || "Impossibile avviare RunningPopulation");
              }
              alert(`${data.message}: ${data.applicationName || data.jobName || "nome non disponibile"}`);
            })
            .catch((err) => alert(`Errore nell'avvio di RunningPopulation: ${err.message}`))
        }
      >
        Avvia RunningPopulation
      </button>
      <button
        onClick={() =>
          fetch(`${apiBaseUrl}/api/v1/startExerciseCorrelation`, { method: "POST" })
            .then((response) => response.json())
            .then((data) => {
              if (!data.success) {
                throw new Error(data.error || "Impossibile avviare ExerciseCorrelation");
              }
              alert(`${data.message}: ${data.applicationName || data.jobName || "nome non disponibile"}`);
            })
            .catch((err) => alert(`Errore nell'avvio di ExerciseCorrelation: ${err.message}`))
        }
      >
        Avvia ExerciseCorrelation
      </button>
    </section>
  );
}
