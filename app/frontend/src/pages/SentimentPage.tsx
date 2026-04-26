import { useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";

type SentimentResponse = { sentiments: Record<string, number>[] };

export function SentimentPage() {
  const [word, setWord] = useState("bandwidth");
  const [searchWord, setSearchWord] = useState("bandwidth");
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 24 * 60 * 60 * 1000);

  const { data, loading, error } = usePolling(
    () =>
      postApi<SentimentResponse, Record<string, string>>("/getSentiments", {
        word: searchWord,
        "range-from": start.toISOString(),
        "range-to": end.toISOString(),
      }),
    [searchWord],
  );

  const points =
    data?.sentiments.map((entry) => {
      const [time, sentiment] = Object.entries(entry)[0];
      return { time, sentiment };
    }) ?? [];

  return (
    <ChartPanel title="Word Sentiment vs Time (10-min buckets)" loading={loading} error={error}>
      <form
        className="controls"
        onSubmit={(e) => {
          e.preventDefault();
          if (word.trim()) setSearchWord(word.trim());
        }}
      >
        <input value={word} onChange={(e) => setWord(e.target.value)} placeholder="Search word" />
        <button type="submit">Search</button>
      </form>
      {points.length === 0 ? (
        <p>No sentiment data found for this word in the selected window.</p>
      ) : (
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={points}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <ReferenceLine y={0} stroke="#4d8bc1" strokeDasharray="5 5" />
            <Line dataKey="sentiment" type="monotone" stroke="#00d4ff" strokeWidth={2.4} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

