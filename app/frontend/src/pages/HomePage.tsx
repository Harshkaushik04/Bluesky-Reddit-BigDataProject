import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
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
    <ChartPanel title="Data Collected vs Time (10-min buckets)" loading={loading} error={error}>
      {combined.length === 0 ? (
        <p>No data in selected time range. Expand your range or ingest newer data.</p>
      ) : (
        <ResponsiveContainer width="100%" height={420}>
          <AreaChart data={combined}>
            <defs>
              <linearGradient id="firehoseGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#36a0ff" stopOpacity={0.8} />
                <stop offset="95%" stopColor="#36a0ff" stopOpacity={0.05} />
              </linearGradient>
              <linearGradient id="getPostsGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#00d4ff" stopOpacity={0.8} />
                <stop offset="95%" stopColor="#00d4ff" stopOpacity={0.05} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke="#1f3b67" />
            <XAxis dataKey="time" stroke="#8cc7ff" tick={{ fontSize: 10 }} />
            <YAxis stroke="#8cc7ff" />
            <Tooltip />
            <Area type="monotone" dataKey="firehose" stroke="#4aa7ff" fill="url(#firehoseGradient)" />
            <Area type="monotone" dataKey="getPosts" stroke="#00d4ff" fill="url(#getPostsGradient)" />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}

