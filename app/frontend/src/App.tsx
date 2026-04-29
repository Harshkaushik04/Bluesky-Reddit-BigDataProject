import { Navigate, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { HomePage } from "./pages/HomePage";
import { SentimentPage } from "./pages/SentimentPage";
import { TrendingTopicsPage } from "./pages/TrendingTopicsPage";
import { ActionRecommendorPage } from "./pages/ActionRecommendorPage";
import { ControversialTopicsPage } from "./pages/ControversialTopicsPage";
import { CrossLinksPage } from "./pages/CrossLinksPage";
import { TrendSaturationPage } from "./pages/TrendSaturationPage";
import { WordPopularityPage } from "./pages/WordPopularityPage";

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/sentiment-analysis" element={<SentimentPage />} />
        <Route path="/trending-topics" element={<TrendingTopicsPage />} />
        <Route path="/word-popularity" element={<WordPopularityPage />} />
        <Route path="/action-recommendor" element={<ActionRecommendorPage />} />
        <Route path="/controversial-topics" element={<ControversialTopicsPage />} />
        <Route path="/cross-links" element={<CrossLinksPage />} />
        <Route path="/trend-saturation" element={<TrendSaturationPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  );
}

export default App;
