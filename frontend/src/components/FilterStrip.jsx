import React from "react";

const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export default function FilterStrip({ year, months, onYearChange, onMonthsChange }) {
  const toggleMonth = (m) => {
    if (months.includes(m)) onMonthsChange(months.filter((x) => x !== m));
    else onMonthsChange([...months, m].sort((a, b) => a - b));
  };

  return (
    <section className="filter-strip">
      <div className="filter-box">
        <p className="filter-title">Year</p>
        <div className="button-row">
          {[
            { key: "overall", label: "Overall" },
            { key: "2025", label: "2025" },
            { key: "2026", label: "2026" }
          ].map((item) => (
            <button
              key={item.key}
              className={`filter-btn ${year === item.key ? "active" : ""}`}
              onClick={() => onYearChange(item.key)}
              type="button"
            >
              {item.label}
            </button>
          ))}
        </div>
      </div>

      <div className="filter-box">
        <p className="filter-title">Months (Multi-select)</p>
        <div className="button-row month-row">
          {MONTH_LABELS.map((label, idx) => {
            const month = idx + 1;
            return (
              <button
                key={label}
                className={`filter-btn ${months.includes(month) ? "active" : ""}`}
                onClick={() => toggleMonth(month)}
                type="button"
              >
                {label}
              </button>
            );
          })}
        </div>
      </div>
    </section>
  );
}

