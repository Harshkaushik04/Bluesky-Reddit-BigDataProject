import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Cell,
} from "recharts";
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
  const barPalette = ["#0ea5ff", "#1ec8ff", "#49b6ff", "#69ddff", "#2389ff", "#83f2ff"];

  return (
    <ChartPanel title="Posts with Reddit links vs Topic" loading={loading} error={error}>
      <div className="controls">
        <label>
          Top N topics
          <input type="number" min={1} max={50} value={topN} onChange={(e) => setTopN(Number(e.target.value))} />
        </label>
      </div>
      <ResponsiveContainer width="100%" height={420}>
        <BarChart data={points} layout="vertical" margin={{ left: 24 }}>
          <CartesianGrid stroke="#1f3b67" />
          <XAxis type="number" stroke="#8cc7ff" />
          <YAxis type="category" dataKey="topic" stroke="#8cc7ff" tick={{ fontSize: 11 }} width={140} />
          <Tooltip />
          <Bar dataKey="num" radius={[0, 8, 8, 0]}>
            {points.map((_, idx) => (
              <Cell key={idx} fill={barPalette[idx % barPalette.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}

