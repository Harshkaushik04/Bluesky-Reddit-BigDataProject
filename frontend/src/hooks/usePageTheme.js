import { useEffect } from "react";

export default function usePageTheme(theme) {
  useEffect(() => {
    document.body.classList.add("page-shell");
    document.body.dataset.theme = theme;
    return () => {
      document.body.dataset.theme = "";
    };
  }, [theme]);
}

