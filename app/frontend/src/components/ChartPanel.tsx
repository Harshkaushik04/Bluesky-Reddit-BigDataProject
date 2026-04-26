import type { PropsWithChildren } from "react";

type Props = PropsWithChildren<{ title: string; error?: string | null; loading?: boolean }>;

export function ChartPanel({ title, error, loading, children }: Props) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h3>{title}</h3>
      </div>
      {loading ? (
        <p>Loading...</p>
      ) : error ? (
        <p className="error">{error}</p>
      ) : (
        children
      )}
    </section>
  );
}

