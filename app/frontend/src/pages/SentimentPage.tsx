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
type WhySentimentsResponse = {
  word: string;
  response: string;
  vectordb_available: boolean;
  vectordb_error?: string | null;
};

type RetrievePostsResponse = {
  word: string;
  retrieved_texts: string[];
  vectordb_error?: string | null;
};

export function SentimentPage() {
  const [word, setWord] = useState("bandwidth");
  const [searchWord, setSearchWord] = useState("bandwidth");
  
  // States for the two-step generation process
  const [retrievedPosts, setRetrievedPosts] = useState<string[] | null>(null);
  const [retrieveLoading, setRetrieveLoading] = useState(false);
  const [whyLoading, setWhyLoading] = useState(false);
  const [whyError, setWhyError] = useState<string | null>(null);
  const [whyText, setWhyText] = useState<string | null>(null);
  
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 24 * 60 * 60 * 1000);
  const rangeFrom = start.toISOString();
  const rangeTo = end.toISOString();

  // Poll for the standard sentiment graph
  const { data, loading, error } = usePolling(
    () =>
      postApi<SentimentResponse, Record<string, string>>("/getSentiments", {
        word: searchWord,
        "range-from": rangeFrom,
        "range-to": rangeTo,
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
          // Reset states when searching a new word
          setWhyText(null);
          setWhyError(null);
          setRetrievedPosts(null);
        }}
      >
        <input value={word} onChange={(e) => setWord(e.target.value)} placeholder="Search word" />
        <button type="submit">Search</button>
        
        {/* BUTTON 1: Retrieve Posts (Starts Qdrant, searches, stops Qdrant) */}
        <button
          type="button"
          disabled={retrieveLoading || !searchWord.trim()}
          onClick={async () => {
            setRetrieveLoading(true);
            setWhyError(null);
            setWhyText(null);
            try {
              const res = await postApi<RetrievePostsResponse, Record<string, any>>("/retrievePosts", {
                word: searchWord,
                limit: 5,
              });
              
              if (res.vectordb_error && (!res.retrieved_texts || res.retrieved_texts.length === 0)) {
                setWhyError(`VectorDB Error: ${res.vectordb_error}`);
              } else if (!res.retrieved_texts || res.retrieved_texts.length === 0) {
                setWhyError("No posts were found for this topic.");
              } else {
                // Store the retrieved posts in the local React state array
                setRetrievedPosts(res.retrieved_texts);
              }
            } catch (err: any) {
              const msg = err?.response?.data?.detail ?? err?.message ?? "Failed to retrieve posts.";
              setWhyError(String(msg));
              setRetrievedPosts(null);
            } finally {
              setRetrieveLoading(false);
            }
          }}
        >
          {retrieveLoading ? "Starting Qdrant & Retrieving..." : "Retrieve posts"}
        </button>

        {/* BUTTON 2: Generate Answer (Sends cached posts to LM Studio) */}
        <button
          type="button"
          disabled={whyLoading || !searchWord.trim() || !retrievedPosts?.length}
          onClick={async () => {
            setWhyLoading(true);
            setWhyError(null);
            setWhyText(null);
            try {
              const res = await postApi<WhySentimentsResponse, Record<string, any>>("/why-sentiments", {
                word: searchWord,
                "range-from": rangeFrom,
                "range-to": rangeTo,
                sample_points: 24,
                retrieved_texts: retrievedPosts, // Pass the array we saved in step 1
              });
              setWhyText(res.response);
              
              if (!res.vectordb_available && res.vectordb_error) {
                setWhyError(`Note: ${res.vectordb_error}`);
              }
            } catch (err: any) {
              const msg = err?.response?.data?.detail ?? err?.message ?? "Failed to reach LM Studio.";
              setWhyError(String(msg));
            } finally {
              setWhyLoading(false);
            }
          }}
        >
          {whyLoading ? "Generating with LM Studio..." : "Generate answer"}
        </button>
      </form>

      {/* Error Displays */}
      {whyError ? <p className="error">{whyError}</p> : null}
      
      {/* Success Indicator for Step 1 */}
      {retrievedPosts && !whyText && !whyLoading && !whyError ? (
         <div style={{ marginTop: 10, color: "#8cc7ff", fontSize: "0.9rem" }}>
           Successfully stored {retrievedPosts.length} posts. Qdrant stopped. Ready to generate context.
         </div>
      ) : null}

      {/* Final LM Studio Output */}
      {whyText ? (
        <div style={{ whiteSpace: "pre-wrap", marginTop: 15, padding: "12px", backgroundColor: "#0b1b32", border: "1px solid #1f3b67", borderRadius: "8px", color: "#cfeaff", lineHeight: "1.5" }}>
          {whyText}
        </div>
      ) : null}

      {/* Sentiment Chart */}
      {points.length === 0 ? (
        <p style={{marginTop: 20}}>No sentiment data found for this word in the selected window.</p>
      ) : (
        <ResponsiveContainer width="100%" height={400} style={{marginTop: 20}}>
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