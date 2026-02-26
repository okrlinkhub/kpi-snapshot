import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ConvexProvider, ConvexReactClient } from "convex/react";
import "./index.css";
import App from "./App.tsx";

const convexUrl = import.meta.env.VITE_CONVEX_URL as string | undefined;

function ConfigError() {
  return (
    <div style={{
      padding: "2rem",
      maxWidth: "32rem",
      margin: "2rem auto",
      fontFamily: "system-ui, sans-serif",
      border: "1px solid #e11",
      borderRadius: "8px",
      background: "#fef2f2",
    }}>
      <h1 style={{ margin: "0 0 0.5rem", fontSize: "1.25rem", color: "#b91c1c" }}>
        Configurazione mancante
      </h1>
      <p style={{ margin: 0, color: "#991b1b" }}>
        <code>VITE_CONVEX_URL</code> non è impostato. Aggiungilo in <code>.env.local</code> nella root del progetto (es.{" "}
        <code>VITE_CONVEX_URL=https://tuo-deployment.convex.cloud</code>), poi riavvia <code>npm run dev</code>.
      </p>
      <p style={{ margin: "0.75rem 0 0", fontSize: "0.875rem", color: "#7f1d1d" }}>
        Vedi anche{" "}
        <a href="https://docs.convex.dev/production/hosting/" target="_blank" rel="noreferrer">documentazione Convex</a>.
      </p>
    </div>
  );
}

if (!convexUrl?.trim()) {
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <ConfigError />
    </StrictMode>,
  );
} else {
  const convex = new ConvexReactClient(convexUrl);
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <ConvexProvider client={convex}>
        <App />
      </ConvexProvider>
    </StrictMode>,
  );
}
