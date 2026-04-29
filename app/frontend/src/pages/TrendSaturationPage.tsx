import { useMemo, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";
import { seriesColor } from "../utils/chartColors";

type Response = { data: Record<string, Record<string, number>[]> };
export function TrendSaturationPage() {
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 60 * 86400000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));
  const [numTopics, setNumTopics] = useState(8);
  const [activeWord, setActiveWord] = useState<string | null>(null);
  const { data, loading, error } = usePolling(
    () =>
      postApi<Response, Record<string, string | number>>("/getTrendSaturation", {
        top_n_words: numTopics,
        "range-from": new Date(from).toISOString(),
        "range-to": new Date(to).toISOString(),
      }),
    [from, to, numTopics],
  );

  const byWord = data?.data ?? {};
  const chartRows = useMemo(() => {
    const rowsMap = new Map<string, Record<string, string | number>>();
    const words = Object.keys(byWord);

    Object.entries(byWord).forEach(([word, points]) => {
      points.forEach((entry) => {
        const [time, val] = Object.entries(entry)[0];
        const row = rowsMap.get(time) ?? { time };
        row[word] = val;
        rowsMap.set(time, row);
      });
    });

    const rows = Array.from(rowsMap.values()).sort(
      (a, b) => new Date(String(a.time)).getTime() - new Date(String(b.time)).getTime(),
    );

    // Fill sparse points for cleaner trajectories.
    rows.forEach((row) => {
      words.forEach((word) => {
        if (row[word] === undefined) row[word] = 0;
      });
    });

    // Peak-focused smoothing: keep local highs and suppress noisy dips.
    words.forEach((word) => {
      const raw = rows.map((row) => Number(row[word] ?? 0));
      const peak = raw.map((val, i) => Math.max(val, raw[i - 1] ?? val, raw[i + 1] ?? val));
      rows.forEach((row, i) => {
        row[word] = peak[i];
      });
    });

    return rows;
  }, [byWord]);

  const visibleWords = useMemo(() => {
    const words = Object.keys(byWord);
    if (!activeWord) return words;
    return words.filter((w) => w === activeWord);
  }, [byWord, activeWord]);

  return (
    <ChartPanel title="Trend Saturation Monitor" loading={loading} error={error}>
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
          Num topics
          <input
            type="number"
            min={1}
            max={50}
            value={numTopics}
            onChange={(e) => setNumTopics(Number(e.target.value))}
          />
        </label>
        <button type="button" onClick={() => setActiveWord(null)}>
          All
        </button>
      </div>
      {chartRows.length === 0 ? (
        <p>No trend-saturation data found for this time window.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <LineChart data={chartRows}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} minTickGap={36} interval="preserveStartEnd" />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <Legend
              onClick={(e) => {
                const key = String(e.dataKey ?? "");
                setActiveWord((prev) => (prev === key ? null : key));
              }}
            />
            {visibleWords.map((word, idx) => (
              <Line
                key={word}
                type="monotone"
                dataKey={word}
                stroke={seriesColor(idx)}
                strokeWidth={2.6}
                dot={false}
                connectNulls
                activeDot={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

