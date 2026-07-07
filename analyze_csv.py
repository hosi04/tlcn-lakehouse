import csv
from pathlib import Path

csv_path = Path("experiments/results/nl2sql_benchmark.csv")
out_path = Path("scratch_failed_analysis.md")

with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    failed = [row for row in reader if row['passed'] == 'False']

with open(out_path, 'w', encoding='utf-8') as f:
    f.write("# Failed Queries Analysis\n\n")
    for row in failed:
        f.write(f"### {row['id']} ({row['difficulty']})\n")
        f.write(f"- **Query**: {row['query']}\n")
        f.write(f"- **Reason**: {row['eval_reason']}\n")
        f.write(f"- **Error**: {row.get('error', '')}\n")
        f.write(f"```sql\n{row['sql']}\n```\n\n")

print(f"Analysis written to {out_path.absolute()}")
