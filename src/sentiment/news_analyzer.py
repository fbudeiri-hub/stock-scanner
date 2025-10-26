"""
News Sentiment Analyzer - Minimal new module
Uses existing NewsAPI integration, minimal code changes
"""
import os
import logging
from typing import Optional
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class NewsAnalyzer:
    """Analyze news sentiment for a stock symbol."""
    
    def __init__(self):
        self.newsapi_key = os.getenv('NEWSAPI_KEY')
        self.base_url = "https://newsapi.org/v2/everything"
        
        # Sentiment keywords
        self.positive_keywords = [
            'surge', 'beat', 'rally', 'upgrade', 'profit', 'gain',
            'strong', 'bullish', 'positive', 'success', 'growth',
            'record', 'soar', 'spike', 'breakout', 'momentum'
        ]
        
        self.negative_keywords = [
            'plunge', 'miss', 'downgrade', 'loss', 'decline',
            'weak', 'bearish', 'negative', 'failure', 'warning',
            'crash', 'slump', 'tumble', 'selloff', 'concern'
        ]
    
    def get_sentiment_score(self, symbol: str, days: int = 7) -> float:
        """
        Get sentiment score for symbol (-100 to +100)
        
        Returns:
            float: -100 (very negative) to +100 (very positive)
        """
        if not self.newsapi_key:
            logger.warning(f"No NEWSAPI_KEY - returning neutral sentiment for {symbol}")
            return 0.0
        
        try:
            # Fetch news
            articles = self._fetch_news(symbol, days)
            if not articles:
                return 0.0
            
            # Calculate sentiment with time decay
            total_sentiment = 0
            total_weight = 0
            
            for article in articles:
                sentiment = self._analyze_article(article)
                
                # Time decay: recent news weighted more
                pub_date = self._parse_date(article.get('publishedAt', ''))
                days_old = (datetime.utcnow() - pub_date).days
                weight = 0.95 ** days_old  # Exponential decay
                
                total_sentiment += sentiment * weight
                total_weight += weight
            
            if total_weight > 0:
                final_score = (total_sentiment / total_weight) * 100
                return max(-100, min(100, final_score))
            
            return 0.0
        
        except Exception as e:
            logger.warning(f"Error getting sentiment for {symbol}: {e}")
            return 0.0
    
    def _fetch_news(self, symbol: str, days: int = 7) -> list:
        """Fetch articles from NewsAPI."""
        try:
            from_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            params = {
                'q': symbol,
                'from': from_date,
                'sortBy': 'publishedAt',
                'language': 'en',
                'apiKey': self.newsapi_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            return data.get('articles', [])[:50]  # Limit to 50 articles
        
        except Exception as e:
            logger.warning(f"Error fetching news for {symbol}: {e}")
            return []
    
    def _analyze_article(self, article: dict) -> float:
        """
        Analyze article for sentiment.
        
        Returns:
            float: -1.0 (very negative) to +1.0 (very positive)
        """
        title = article.get('title', '').lower()
        description = article.get('description', '').lower()
        text = f"{title} {description}"
        
        positive_count = sum(1 for kw in self.positive_keywords if kw in text)
        negative_count = sum(1 for kw in self.negative_keywords if kw in text)
        
        if positive_count + negative_count == 0:
            return 0.0
        
        # Simple sentiment: (positive - negative) / total
        return (positive_count - negative_count) / (positive_count + negative_count)
    
    @staticmethod
    def _parse_date(date_string: str) -> datetime:
        """Parse ISO format date."""
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        except:
            return datetime.utcnow()
