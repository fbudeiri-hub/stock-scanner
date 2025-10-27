import pandas as pd
import os

def generate_dashboard(csv_path, output_html='data/output/dashboard.html'):
    df = pd.read_csv(csv_path)
    top_100 = df.head(100)
    
    html = f"""
    <!DOCTYPE html>
    <html><head><title>Stock Scanner Dashboard</title>
    <style>
    body {{font-family: Arial; margin: 20px; background: #f5f5f5;}}
    h1 {{color: #333;}}
    table {{border-collapse: collapse; width: 100%; background: white;}}
    th {{background: #4CAF50; color: white; padding: 12px; text-align: left;}}
    td {{padding: 10px; border-bottom: 1px solid #ddd;}}
    tr:hover {{background-color: #f5f5f5;}}
    </style></head><body>
    <h1>📊 Top 100 Stocks - Momentum Scanner</h1>
    <p><strong>Total Analyzed:</strong> {len(df)} stocks</p>
    {top_100.to_html(classes='table', index=False)}
    </body></html>
    """
    os.makedirs(os.path.dirname(output_html), exist_ok=True)
    with open(output_html, 'w') as f:
        f.write(html)
    print(f"Dashboard saved: {output_html}")
