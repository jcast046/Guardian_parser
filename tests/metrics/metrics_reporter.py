"""Metrics reporting and comparison functionality for Guardian Parser Pack.

Provides utilities for generating JSON, CSV, and HTML reports comparing
Legacy and LLM parser metrics.
"""
import json
import csv
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime


def generate_json_report(metrics: Dict[str, Any], output_path: str) -> None:
    """Generate a JSON metrics report.

    Args:
        metrics: Dictionary of metrics to report.
        output_path: Path to output JSON file.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "metrics": metrics
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)


def generate_csv_report(metrics: Dict[str, Any], output_path: str) -> None:
    """Generate a CSV metrics report.

    Args:
        metrics: Dictionary of metrics to report.
        output_path: Path to output CSV file.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Flatten metrics for CSV
    rows = []
    for key, value in metrics.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                rows.append({
                    "metric": f"{key}.{sub_key}",
                    "value": sub_value
                })
        else:
            rows.append({
                "metric": key,
                "value": value
            })
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=["metric", "value"])
            writer.writeheader()
            writer.writerows(rows)


def generate_html_dashboard(legacy_metrics: Dict[str, Any],
                           llm_metrics: Dict[str, Any],
                           output_path: str) -> None:
    """Generate an HTML dashboard comparing Legacy vs. LLM metrics.

    Args:
        legacy_metrics: Metrics from Legacy parser.
        llm_metrics: Metrics from LLM parser.
        output_path: Path to output HTML file.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Guardian Parser Metrics Dashboard</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            margin-bottom: 20px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        .metric-name {{
            font-weight: bold;
        }}
        .better {{
            background-color: #d4edda;
        }}
        .worse {{
            background-color: #f8d7da;
        }}
    </style>
</head>
<body>
    <h1>Guardian Parser Metrics Dashboard</h1>
    <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    
    <h2>Comparison: Legacy vs. LLM</h2>
    <table>
        <tr>
            <th>Metric</th>
            <th>Legacy</th>
            <th>LLM</th>
            <th>Difference</th>
        </tr>
"""
    
    # Compare metrics
    all_metrics = set(legacy_metrics.keys()) | set(llm_metrics.keys())
    
    for metric in sorted(all_metrics):
        legacy_value = legacy_metrics.get(metric, "N/A")
        llm_value = llm_metrics.get(metric, "N/A")
        
        # Calculate difference if both are numbers
        try:
            if isinstance(legacy_value, (int, float)) and isinstance(llm_value, (int, float)):
                diff = llm_value - legacy_value
                diff_str = f"{diff:+.4f}"
                # Highlight better/worse (higher is better for most metrics)
                if metric in ["f1_score", "completeness", "accuracy", "schema_compliance_rate", 
                             "geocoding_cache_hit_rate"]:
                    row_class = "better" if diff > 0 else "worse" if diff < 0 else ""
                elif metric in ["repair_rate", "ocr_fallback_rate", "latency_mean"]:
                    row_class = "better" if diff < 0 else "worse" if diff > 0 else ""
                else:
                    row_class = ""
            else:
                diff_str = "N/A"
                row_class = ""
        except (TypeError, ValueError):
            diff_str = "N/A"
            row_class = ""
        
        html += f"""
        <tr class="{row_class}">
            <td class="metric-name">{metric}</td>
            <td>{legacy_value}</td>
            <td>{llm_value}</td>
            <td>{diff_str}</td>
        </tr>
"""
    
    html += """
    </table>
</body>
</html>
"""
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)


def compare_metrics(legacy_metrics: Dict[str, Any],
                   llm_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Compare Legacy and LLM metrics side-by-side.

    Args:
        legacy_metrics: Metrics from Legacy parser.
        llm_metrics: Metrics from LLM parser.

    Returns:
        Dictionary with comparison results including differences and
        percent changes.
    """
    comparison = {
        "legacy": legacy_metrics,
        "llm": llm_metrics,
        "differences": {}
    }
    
    all_metrics = set(legacy_metrics.keys()) | set(llm_metrics.keys())
    
    for metric in all_metrics:
        legacy_value = legacy_metrics.get(metric)
        llm_value = llm_metrics.get(metric)
        
        if legacy_value is not None and llm_value is not None:
            try:
                if isinstance(legacy_value, (int, float)) and isinstance(llm_value, (int, float)):
                    diff = llm_value - legacy_value
                    comparison["differences"][metric] = {
                        "legacy": legacy_value,
                        "llm": llm_value,
                        "difference": diff,
                        "percent_change": (diff / legacy_value * 100) if legacy_value != 0 else 0.0
                    }
            except (TypeError, ValueError):
                pass
    
    return comparison


def generate_comparison_report(legacy_metrics: Dict[str, Any],
                              llm_metrics: Dict[str, Any],
                              output_dir: str) -> None:
    """Generate all comparison reports (JSON, CSV, HTML) in output directory.

    Args:
        legacy_metrics: Metrics from Legacy parser.
        llm_metrics: Metrics from LLM parser.
        output_dir: Directory to output reports.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate comparison
    comparison = compare_metrics(legacy_metrics, llm_metrics)
    
    # Generate JSON report
    generate_json_report(comparison, str(output_path / "metrics_comparison.json"))
    
    # Generate CSV reports
    generate_csv_report(legacy_metrics, str(output_path / "legacy_metrics.csv"))
    generate_csv_report(llm_metrics, str(output_path / "llm_metrics.csv"))
    
    # Generate HTML dashboard
    generate_html_dashboard(legacy_metrics, llm_metrics, 
                          str(output_path / "metrics_dashboard.html"))
    
    print(f"Metrics reports generated in {output_dir}")
    print(f"  - metrics_comparison.json")
    print(f"  - legacy_metrics.csv")
    print(f"  - llm_metrics.csv")
    print(f"  - metrics_dashboard.html")


def track_metrics_over_time(metrics_history: List[Dict[str, Any]],
                           output_path: str) -> None:
    """Track metrics over time for CI/CD integration.

    Args:
        metrics_history: List of metrics dictionaries with timestamps.
        output_path: Path to output JSON file.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    history = {
        "timestamp": datetime.now().isoformat(),
        "metrics_history": metrics_history
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)

