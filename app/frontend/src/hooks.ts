import { useEffect, useRef, useState } from "react";

export function usePolling<T>(
  fetcher: () => Promise<T>,
  deps: unknown[],
  intervalMs = 10_000,
) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    let timer: number | undefined;

    const run = async () => {
      try {
        const result = await fetcher();
        if (!mounted.current) return;
        setData(result);
        setError(null);
      } catch (err) {
        if (!mounted.current) return;
        setError(err instanceof Error ? err.message : "Request failed.");
      } finally {
        if (mounted.current) setLoading(false);
      }
    };

    void run();
    timer = window.setInterval(run, intervalMs);
    return () => {
      mounted.current = false;
      if (timer) window.clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return { data, loading, error };
}

