import { useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";

type Response = { popularity: Record<string, number>[] };

export function WordPopularityPage() {
  const now = new Date();
  const [word, setWord] = useState("bandwidth");
  const [searchWord, setSearchWord] = useState("bandwidth");
  const [from, setFrom] = useState(new Date(now.getTime() - 60 * 86400000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));

  const { data, loading, error } = usePolling(
    () =>
      postApi<Response, Record<string, string>>("/getWordPopularityTimeline", {
        word: searchWord,
        "range-from": new Date(from).toISOString(),
        "range-to": new Date(to).toISOString(),
      }),
    [searchWord, from, to],
  );

  const points =
    data?.popularity
      .map((entry) => {
        const [time, popularity] = Object.entries(entry)[0];
        return { time, popularity };
      })
      .sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime()) ?? [];

  return (
    <ChartPanel title="Word Popularity vs Time (10-min buckets)" loading={loading} error={error}>
      <form
        className="controls"
        onSubmit={(e) => {
          e.preventDefault();
          if (word.trim()) setSearchWord(word.trim());
        }}
      >
        <input value={word} onChange={(e) => setWord(e.target.value)} placeholder="Search word" />
        <label>
          Start
          <input type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} />
        </label>
        <label>
          End
          <input type="datetime-local" value={to} onChange={(e) => setTo(e.target.value)} />
        </label>
        <button type="submit">Search</button>
      </form>

      {points.length === 0 ? (
        <p>No popularity timeline data found for this word in the selected range.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <LineChart data={points}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <Line type="monotone" dataKey="popularity" stroke="#00d4ff" strokeWidth={2.5} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

