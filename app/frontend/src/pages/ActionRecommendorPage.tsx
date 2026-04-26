import { useState } from "react";
import type { FormEvent } from "react";
import { postApi } from "../api";
import { ChartPanel } from "../components/ChartPanel";

export function ActionRecommendorPage() {
  const [sentence, setSentence] = useState("");
  const [response, setResponse] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const data = await postApi<{ response: string }, { sentence: string }>("/actionRecommend", { sentence });
      setResponse(data.response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Request failed.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <ChartPanel title="Action Recommendor" loading={loading} error={error}>
      <form className="controls column" onSubmit={onSubmit}>
        <textarea
          value={sentence}
          onChange={(e) => setSentence(e.target.value)}
          rows={5}
          placeholder="Enter sentence..."
        />
        <button type="submit">Recommend</button>
      </form>
      {response && <pre className="response-box">{response}</pre>}
    </ChartPanel>
  );
}

