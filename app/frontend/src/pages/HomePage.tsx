import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { postApi } from "../api";
import { usePolling } from "../hooks";
import { ChartPanel } from "../components/ChartPanel";

type DataResponse = {
  firehose_collected: { collected: Record<string, number>[] };
  getPosts_collected: { collected: Record<string, number>[] };
};

function getDefaultRange() {
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 24 * 60 * 60 * 1000);
  return { "range-from": start.toISOString(), "range-to": end.toISOString() };
}

export function HomePage() {
  const { data, loading, error } = usePolling(
    () => postApi<DataResponse, Record<string, string>>("/getDataCollectedStats", getDefaultRange()),
    [],
  );

  const combined =
    data?.firehose_collected.collected.map((item, idx) => {
      const [time, firehose] = Object.entries(item)[0];
      const getPostsItem = data.getPosts_collected.collected[idx] ?? {};
      const getPosts = Object.values(getPostsItem)[0] ?? 0;
      return { time, firehose, getPosts };
    }) ?? [];

  return (
    <ChartPanel title="Data Collected vs Time (2h buckets)" loading={loading} error={error}>
      {combined.length === 0 ? (
        <p>No data in selected time range. Expand your range or ingest newer data.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <BarChart data={combined}>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <Bar dataKey="firehose" fill="#1d7cff" />
            <Bar dataKey="getPosts" fill="#00d4ff" />
          </BarChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

