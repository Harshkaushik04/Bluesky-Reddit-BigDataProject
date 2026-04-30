import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import RedditPage from "./pages/RedditPage.jsx";
import BlueskyPage from "./pages/BlueskyPage.jsx";
import ComparePage from "./pages/ComparePage.jsx";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<RedditPage />} />
      <Route path="/bluesky" element={<BlueskyPage />} />
      <Route path="/compare" element={<ComparePage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
