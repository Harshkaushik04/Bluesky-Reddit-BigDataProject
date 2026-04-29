import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";
import { seriesColor } from "../utils/chartColors";

type Topic = { topic_name: string; average_like_to_comment_ratio: number };
type Response = { ranges: { range: string; topics: Topic[] }[] };
export function ControversialTopicsPage() {
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 60 * 86400000).toISOString().slice(0, 16));
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

  const barData = useMemo(() => {
    const aggregate = new Map<string, { total: number; count: number }>();
    (data?.ranges ?? []).forEach((rangeEntry) => {
      rangeEntry.topics.forEach((topic) => {
        const prev = aggregate.get(topic.topic_name) ?? { total: 0, count: 0 };
        aggregate.set(topic.topic_name, {
          total: prev.total + topic.average_like_to_comment_ratio,
          count: prev.count + 1,
        });
      });
    });

    return Array.from(aggregate.entries())
      .map(([name, stats]) => ({
        name,
        value: Number((stats.total / stats.count).toFixed(2)),
      }))
      .sort((a, b) => b.value - a.value)
      .slice(0, topN);
  }, [data, topN]);

  return (
    <ChartPanel title="Controversial Topics (avg ratio across selected range)" loading={loading} error={error}>
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
      {barData.length === 0 ? (
        <p>No controversial topics were returned for this range.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <BarChart data={barData} layout="vertical" margin={{ left: 20, right: 12 }}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis type="number" stroke="#8cc7ff" />
            <YAxis type="category" dataKey="name" stroke="#8cc7ff" tick={{ fontSize: 11 }} width={180} />
            <Tooltip />
            <Bar dataKey="value" radius={[0, 8, 8, 0]}>
              {barData.map((_, idx) => (
                <Cell key={idx} fill={seriesColor(idx)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

