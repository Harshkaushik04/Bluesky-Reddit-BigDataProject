import { useMemo, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";
import { seriesColor } from "../utils/chartColors";

type WordSeries = { word: string; popularity: { time_range: string; popularity: number }[] };
type Response = { words: WordSeries[] };

export function TrendingTopicsPage() {
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 60 * 86400000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));
  const [numWords, setNumWords] = useState(8);

  const { data, loading, error } = usePolling(
    () =>
      postApi<Response, Record<string, string | number>>("/popularWordsByTime", {
        "range-from": new Date(from).toISOString(),
        "range-to": new Date(to).toISOString(),
        num_words: numWords,
      }),
    [from, to, numWords],
  );

  const chartData = useMemo(() => {
    const words = (data?.words ?? []).map((series) => series.word);
    const byTime = new Map<string, Record<string, string | number>>();

    (data?.words ?? []).forEach((series) => {
      series.popularity.forEach((pt) => {
        const row = byTime.get(pt.time_range) ?? { time: pt.time_range };
        row[series.word] = pt.popularity;
        byTime.set(pt.time_range, row);
      });
    });

    const orderedRows = Array.from(byTime.values()).sort(
      (a, b) => new Date(String(a.time)).getTime() - new Date(String(b.time)).getTime(),
    );

    // For sparse words, show 0 instead of gaps so lines stay readable.
    orderedRows.forEach((row) => {
      words.forEach((word) => {
        if (row[word] === undefined) row[word] = 0;
      });
    });

    return orderedRows;
  }, [data]);

  return (
    <ChartPanel title="Trending Topic Popularity vs Time" loading={loading} error={error}>
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
          Num words
          <input
            type="number"
            min={1}
            max={50}
            value={numWords}
            onChange={(e) => setNumWords(Number(e.target.value))}
          />
        </label>
      </div>
      {chartData.length === 0 ? (
        <p>No trending-topic points found for this range.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <LineChart data={chartData}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} minTickGap={36} interval="preserveStartEnd" />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <Legend />
            {(data?.words ?? []).map((series, idx) => (
              <Line
                key={series.word}
                type="monotone"
                dataKey={series.word}
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

