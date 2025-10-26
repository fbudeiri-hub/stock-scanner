from src.analysis.dashboard import DashboardGenerator
import pandas as pd
import os

print("TEST 6: Dashboard Generator")
print("=" * 70)

# Load your latest scan results
results_file = 'data/scan_results_20251026_174722.csv'

if os.path.exists(results_file):
    results = pd.read_csv(results_file)
    
    # Generate dashboard
    gen = DashboardGenerator()
    html_file = gen.generate_html(results, 'index.html')
    
    print(f"\nDashboard generated successfully!")
    print(f"   File: {html_file}")
    print(f"   Open in browser: file://{os.path.abspath(html_file)}")
else:
    print(f"Results file not found: {results_file}")
