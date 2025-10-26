import pandas as pd
from datetime import datetime
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DashboardGenerator:
    """
    Generate beautiful HTML dashboard for scan results.
    
    Features:
    - Top 100 stocks ranked by momentum score
    - Signal distribution (BUY, NEUTRAL, SELL)
    - Summary statistics
    - Interactive sorting and filtering
    - Responsive design for desktop/mobile
    """
    
    @staticmethod
    def generate_html(results_df: pd.DataFrame, output_file: str = 'dashboard.html') -> str:
        """
        Generate HTML dashboard from scan results.
        
        Args:
            results_df: Results DataFrame from scanner
            output_file: Output HTML filepath
            
        Returns:
            Path to generated HTML file
        """
        if results_df.empty:
            logger.warning("No results to display")
            return ""
        
        # Sort by momentum score
        top_100 = results_df.head(100).sort_values('momentum_score', ascending=False)
        
        # Calculate statistics
        total_stocks = len(top_100)
        avg_score = top_100['momentum_score'].mean()
        buy_count = len(top_100[top_100['signal'] == 'BUY'])
        neutral_count = len(top_100[top_100['signal'] == 'NEUTRAL'])
        sell_count = len(top_100[top_100['signal'] == 'SELL'])
        
        # Create HTML
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stock Scanner Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        
        .header h1 {{
            color: #333;
            margin-bottom: 10px;
            font-size: 28px;
        }}
        
        .header p {{
            color: #666;
            font-size: 14px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            text-align: center;
        }}
        
        .stat-card h3 {{
            color: #666;
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .stat-card .value {{
            font-size: 32px;
            font-weight: 700;
            margin-bottom: 5px;
        }}
        
        .stat-card .total {{
            color: #667eea;
        }}
        
        .stat-card .avg {{
            color: #667eea;
        }}
        
        .stat-card .buy {{
            color: #10b981;
        }}
        
        .stat-card .neutral {{
            color: #f59e0b;
        }}
        
        .stat-card .sell {{
            color: #ef4444;
        }}
        
        .results-section {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            overflow-x: auto;
        }}
        
        .results-section h2 {{
            color: #333;
            margin-bottom: 20px;
            font-size: 20px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        
        thead {{
            background: #f8f9fa;
            border-bottom: 2px solid #e0e0e0;
        }}
        
        th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
            color: #333;
            cursor: pointer;
            user-select: none;
        }}
        
        td {{
            padding: 15px;
            border-bottom: 1px solid #f0f0f0;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .symbol {{
            font-weight: 600;
            color: #667eea;
        }}
        
        .price {{
            color: #333;
        }}
        
        .score {{
            font-weight: 600;
            font-size: 16px;
        }}
        
        .score.excellent {{
            color: #10b981;
        }}
        
        .score.good {{
            color: #06b6d4;
        }}
        
        .score.neutral {{
            color: #f59e0b;
        }}
        
        .score.poor {{
            color: #ef4444;
        }}
        
        .signal {{
            padding: 4px 12px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 12px;
            text-transform: uppercase;
            text-align: center;
            width: 100px;
        }}
        
        .signal.buy {{
            background: #d1fae5;
            color: #065f46;
        }}
        
        .signal.neutral {{
            background: #fef3c7;
            color: #92400e;
        }}
        
        .signal.sell {{
            background: #fee2e2;
            color: #7f1d1d;
        }}
        
        .rsi {{
            color: #667eea;
        }}
        
        .footer {{
            text-align: center;
            margin-top: 30px;
            color: white;
            font-size: 12px;
        }}
        
        @media (max-width: 768px) {{
            .results-section {{
                padding: 15px;
            }}
            
            table {{
                font-size: 12px;
            }}
            
            th, td {{
                padding: 10px 5px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Stock Scanner Dashboard</h1>
            <p>Real-time momentum analysis • Generated {datetime.now().strftime('%B %d, %Y at %H:%M:%S UTC')}</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Stocks</h3>
                <div class="value total">{total_stocks}</div>
            </div>
            <div class="stat-card">
                <h3>Avg Score</h3>
                <div class="value avg">{avg_score:.1f}</div>
            </div>
            <div class="stat-card">
                <h3>🟢 Buy Signals</h3>
                <div class="value buy">{buy_count}</div>
            </div>
            <div class="stat-card">
                <h3>🟡 Neutral</h3>
                <div class="value neutral">{neutral_count}</div>
            </div>
            <div class="stat-card">
                <h3>🔴 Sell Signals</h3>
                <div class="value sell">{sell_count}</div>
            </div>
        </div>
        
        <div class="results-section">
            <h2>Top 100 Momentum Stocks</h2>
            <table>
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Symbol</th>
                        <th>Price</th>
                        <th>Momentum Score</th>
                        <th>Signal</th>
                        <th>RSI(14)</th>
                    </tr>
                </thead>
                <tbody>
"""
        
        # Add rows
        for idx, (_, row) in enumerate(top_100.iterrows(), 1):
            score = row['momentum_score']
            if score >= 80:
                score_class = 'excellent'
            elif score >= 60:
                score_class = 'good'
            elif score >= 40:
                score_class = 'neutral'
            else:
                score_class = 'poor'
            
            signal_lower = row['signal'].lower()
            price = f"${row['close']:.2f}"
            rsi = f"{row['rsi_14']:.1f}"
            
            html += f"""
                    <tr>
                        <td>#{idx}</td>
                        <td class="symbol">{row['symbol']}</td>
                        <td class="price">{price}</td>
                        <td class="score {score_class}">{score:.1f}</td>
                        <td><span class="signal {signal_lower}">{row['signal']}</span></td>
                        <td class="rsi">{rsi}</td>
                    </tr>
"""
        
        html += """
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>Stock Scanner • Powered by yfinance • Data updated in real-time</p>
        </div>
    </div>
</body>
</html>
"""
        
        # Save HTML
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"Dashboard saved to {output_file}")
        return output_file


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    # Load results from scanner
    results_file = 'data/scan_results_latest.csv'
    
    try:
        results_df = pd.read_csv(results_file)
        
        # Generate dashboard
        dashboard_gen = DashboardGenerator()
        html_file = dashboard_gen.generate_html(results_df, 'index.html')
        
        print(f"\n✅ Dashboard generated: {html_file}")
        print(f"   Open in browser to view: file://{os.path.abspath(html_file)}")
        
    except FileNotFoundError:
        print(f"Error: {results_file} not found")
