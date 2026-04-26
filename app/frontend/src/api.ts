import axios from "axios";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export const api = axios.create({
  baseURL: API_BASE_URL,
  headers: { "Content-Type": "application/json" },
});

export const postApi = async <TResponse, TPayload>(
  endpoint: string,
  payload: TPayload,
): Promise<TResponse> => {
  const { data } = await api.post<TResponse>(endpoint, payload);
  return data;
};

