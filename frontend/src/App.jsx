import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import HomePage from "./pages/HomePage.jsx";
import RedditPage from "./pages/RedditPage.jsx";
import BlueskyPage from "./pages/BlueskyPage.jsx";
import ComparisonPage from "./pages/ComparisonPage.jsx";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/reddit" element={<RedditPage />} />
      <Route path="/bluesky" element={<BlueskyPage />} />
      <Route path="/comparison" element={<ComparisonPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

