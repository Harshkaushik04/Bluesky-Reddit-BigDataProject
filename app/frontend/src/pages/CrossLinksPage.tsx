import { useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";

type Response = { topics: Record<string, number> };

export function CrossLinksPage() {
  const [topN, setTopN] = useState(10);
  const { data, loading, error } = usePolling(
    () => postApi<Response, { top_n_topics: number }>("/getTopCrossTopics", { top_n_topics: topN }),
    [topN],
  );

  const points = Object.entries(data?.topics ?? {}).map(([topic, num]) => ({ topic, num }));

  return (
    <ChartPanel title="Posts with Reddit links vs Topic" loading={loading} error={error}>
      <div className="controls">
        <label>
          Top N topics
          <input type="number" min={1} max={50} value={topN} onChange={(e) => setTopN(Number(e.target.value))} />
        </label>
      </div>
      <ResponsiveContainer width="100%" height={420}>
        <BarChart data={points}>
          <CartesianGrid stroke="#1f3b67" />
          <XAxis dataKey="topic" stroke="#8cc7ff" tick={{ fontSize: 10 }} />
          <YAxis stroke="#8cc7ff" />
          <Tooltip />
          <Bar dataKey="num" fill="#1d7cff" />
        </BarChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}

