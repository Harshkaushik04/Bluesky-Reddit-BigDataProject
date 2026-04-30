const currentHostname = window.location.hostname || "localhost";
const currentProtocol = window.location.protocol === "https:" ? "https:" : "http:";

export const API_ORIGIN =
  import.meta.env.VITE_API_ORIGIN || `${currentProtocol}//${currentHostname}:8000`;

export const BLUESKY_API_ORIGIN =
  import.meta.env.VITE_BLUESKY_API_ORIGIN || `${currentProtocol}//${currentHostname}:8001`;

export function apiUrl(path) {
  return new URL(path, API_ORIGIN).toString();
}

export async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

