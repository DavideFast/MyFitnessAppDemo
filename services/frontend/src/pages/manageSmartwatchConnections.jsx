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

  const avviaELT = () => {
    fetch(`${apiBaseUrl}/api/v1/startELTProcess`, { method: "POST" })
      .then((response) => response.json())
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error}`);
        }
        console.log("ELT avviato:", data);
        alert("ELT avviato. Controlla la console per i dettagli.");
      })
      .catch((err) => {
        console.error("Errore nell'avvio dell'ELT:", err);
        alert(`Errore nell'avvio dell'ELT: ${err.message}`);
      });
  };

  const fermaELT = () => {
    fetch(`${apiBaseUrl}/api/v1/stopELTProcess`, { method: "POST" })
      .then((response) => response.json())
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error}`);
        }
        console.log("ELT fermato:", data);
        alert("ELT fermato. Controlla la console per i dettagli.");
      })
      .catch((err) => {
        console.error("Errore nel fermare l'ELT:", err);
        alert(`Errore nel fermare l'ELT: ${err.message}`);
      });
  };

  return (
    <section aria-label="Dati workouts">
      <h1>Gestione connessioni Smartwatch </h1>
      <button onClick={avviaSimulazione}>Avvia simulazione</button>
      <button onClick={fermaSimulazione}>Ferma simulazione</button>

      <br />
      <br />
      <h1>Gestione processo ELT</h1>
      <button onClick={avviaELT}>Avvia processo ELT</button>
      <button onClick={fermaELT}>Ferma processo ELT</button>

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
    </section>
  );
}
