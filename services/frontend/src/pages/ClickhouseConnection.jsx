import React, { useEffect, useState } from "react";
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, scales } from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

const options = {
  responsive: true,
  plugins: {
    legend: {
      position: "top",
    },
    title: {
      display: true,
      text: "Sessione di corsa",
    },
  },
  scales: {
    y: {
      type: "linear",
    },
  },
};

export default function ClickhouseConnectionPage() {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    const url = "/api/v1/readClickhouse";
    fetch(url)
      .then((response) => {
        return response.json();
      })
      .then((data) => {
        if (!data.success) {
          throw new Error(`Errore nella richiesta: ${data.error}`);
        }
        return data;
      })
      .then((data) => {
        setRows(data.data || []);
      })
      .catch((err) => {
        console.error("Errore nel fetch dei dati della corsa:", err);
      });
  }, []);

  return (
    <section aria-label="Dati corsa">
      <h2>Sessione di corsa</h2>
      <p className="panel-subtitle">Sessione corsa ottenuta prelevando i dati da clickhouse</p>
      <div className="table-wrap charts-wrap">
        <Line options={options} data={{ labels: rows.map((row) => row.timestamp), datasets: [{ label: "Velocità", data: rows.map((row) => row.velocity), borderColor: "rgb(75, 192, 192)", backgroundColor: "rgba(75, 192, 192, 0.5)" }] }} />
        <Line options={options} data={{ labels: rows.map((row) => row.timestamp), datasets: [{ label: "Frequenza cardiaca", data: rows.map((row) => row.heart_rate), borderColor: "rgb(255, 99, 132)", backgroundColor: "rgba(255, 99, 132, 0.5)" }] }} />
        <Line options={options} data={{ labels: rows.map((row) => row.timestamp), datasets: [{ label: "Altitudine", data: rows.map((row) => row.altitude), borderColor: "rgb(255, 99, 132)", backgroundColor: "rgba(255, 99, 132, 0.5)" }] }} />
      </div>
    </section>
  );
}
