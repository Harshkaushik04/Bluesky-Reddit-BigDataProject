import { useMemo, useState } from "react";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";
import { seriesColor } from "../utils/chartColors";

// Combined standard English stop words + custom filler words
const STOP_WORDS = new Set([
  "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves",
  "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
  "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
  "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
  "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about",
  "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up",
  "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when",
  "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor",
  "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now",
  "rt", "amp", "via", "http", "https", "www", "com", "org", "net", "co", "im", "ive", "dont", "didnt", "doesnt",
  "cant", "couldnt", "wouldnt", "shouldnt", "youre", "theyre", "weve", "thats", "u", "ur", "ya", "lol", "lmao",
  "omg", "idk", "btw", "thx", "pls", "okay", "ok", "one"
]);

type WordSeries = { word: string; popularity: { time_range: string; popularity: number }[] };
type Response = { words: WordSeries[] };

export function TrendingTopicsPage() {
  const now = new Date();
  const [from, setFrom] = useState(new Date(now.getTime() - 60 * 86400000).toISOString().slice(0, 16));
  const [to, setTo] = useState(now.toISOString().slice(0, 16));
  const [numWords, setNumWords] = useState(8);
  const [activeWord, setActiveWord] = useState<string | null>(null);

  const { data, loading, error } = usePolling(
    () =>
      postApi<Response, Record<string, string | number>>("/popularWordsByTime", {
        "range-from": new Date(from).toISOString(),
        "range-to": new Date(to).toISOString(),
        num_words: numWords,
      }),
    [from, to, numWords],
  );

  // 1. Filter the raw data to remove stop words
  const filteredWordsData = useMemo(() => {
    return (data?.words ?? []).filter((series) => !STOP_WORDS.has(series.word.toLowerCase()));
  }, [data]);

  const chartData = useMemo(() => {
    // 2. Use filtered data instead of raw data
    const words = filteredWordsData.map((series) => series.word);
    const byTime = new Map<string, Record<string, string | number>>();

    filteredWordsData.forEach((series) => {
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

    // Peak-focused smoothing: keep local highs, reduce frequent sharp dips.
    words.forEach((word) => {
      const raw = orderedRows.map((row) => Number(row[word] ?? 0));
      const peak = raw.map((val, i) => Math.max(val, raw[i - 1] ?? val, raw[i + 1] ?? val));
      orderedRows.forEach((row, i) => {
        row[word] = peak[i];
      });
    });

    return orderedRows;
  }, [filteredWordsData]); // Dependency updated

  const visibleWords = useMemo(() => {
    // 3. Use filtered data instead of raw data
    const words = filteredWordsData.map((series) => series.word);
    if (!activeWord) return words;
    return words.filter((w) => w === activeWord);
  }, [filteredWordsData, activeWord]); // Dependency updated

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
        <button type="button" onClick={() => setActiveWord(null)}>
          All
        </button>
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