import pandas as pd
import os
from typing import Dict, Any, List

class DataProcessor:
    def __init__(self, export_dir: str = "./dune_exports"):
        self.export_dir = export_dir
        os.makedirs(self.export_dir, exist_ok=True)

    def process_results(self, result_data: Any, limit: int = 5) -> Dict[str, Any]:
        """
        Summarizes Dune results using Pandas.
        Expects result_data to be the result object from dune_client 
        (which usually contains .result.rows).
        """
        # Extract rows
        # dune-client 1.x: result_data.result.rows is list of dicts
        try:
            rows = result_data.result.rows
        except AttributeError:
            # Fallback if it's already a list or dict
            rows = result_data if isinstance(result_data, list) else []

        if not rows:
            return {
                "row_count": 0,
                "preview": [],
                "summary": "No data returned."
            }

        df = pd.DataFrame(rows)
        
        # Summary Stats
        summary = {
            "columns": list(df.columns),
            "row_count": len(df),
            "stats": {}
        }
        
        # Calculate lightweight stats for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            summary["stats"][col] = {
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "avg": float(df[col].mean())
            }
            
        return {
            "row_count": len(df),
            "columns": list(df.columns),
            "preview": df.head(limit).to_dict(orient="records"),
            "summary_stats": summary["stats"]
        }

    def export_to_csv(self, result_data: Any, job_id: str) -> str:
        """
        Exports full results to CSV.
        """
        try:
            rows = result_data.result.rows
        except AttributeError:
            rows = result_data if isinstance(result_data, list) else []

        if not rows:
            return "No data to export"

        df = pd.DataFrame(rows)
        filename = f"dune_results_{job_id}.csv"
        filepath = os.path.join(self.export_dir, filename)
        
        df.to_csv(filepath, index=False)
        return filepath
