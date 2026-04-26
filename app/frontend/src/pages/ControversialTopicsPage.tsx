import { useState } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";

type Topic = { topic_name: string; average_like_to_comment_ratio: number };
type Response = { ranges: { range: string; topics: Topic[] }[] };
const colors = ["#1d7cff", "#00d4ff", "#5ba6ff", "#0ec9ff", "#80d8ff", "#3f8cff", "#27d8ff"];

export function ControversialTopicsPage() {
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 7 * 86400000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));
  const [topN, setTopN] = useState(10);

  const { data, loading, error } = usePolling(
    () =>
      postApi<Response, Record<string, string | number>>("/getControversialTopics", {
        "range-from": new Date(from).toISOString(),
        "range-to": new Date(to).toISOString(),
        top_n_words: topN,
      }),
    [from, to, topN],
  );

  const latestRange = data?.ranges[data.ranges.length - 1];
  const pieData =
    latestRange?.topics.map((topic) => ({
      name: topic.topic_name,
      value: topic.average_like_to_comment_ratio,
    })) ?? [];

  return (
    <ChartPanel title="Controversial Topics (latest range in selected window)" loading={loading} error={error}>
      <div className="controls">
        <label>
          Start
          <input type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} />
        </label>
        <label>
          End
          <input type="datetime-local" value={to} onChange={(e) => setTo(e.target.value)} />
        </label>
        <label>
          Top N
          <input type="number" min={1} max={30} value={topN} onChange={(e) => setTopN(Number(e.target.value))} />
        </label>
      </div>
      <ResponsiveContainer width="100%" height={420}>
        <PieChart>
          <Pie data={pieData} dataKey="value" nameKey="name" outerRadius={150} label>
            {pieData.map((_, idx) => (
              <Cell key={idx} fill={colors[idx % colors.length]} />
            ))}
          </Pie>
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}

