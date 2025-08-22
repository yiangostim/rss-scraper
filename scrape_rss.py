import feedparser
import csv
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

def scrape_splash247_rss():
    """Scrape Splash247 RSS feed"""
    feed_url = 'https://splash247.com/feed/'
    articles = []
    
    print("Scraping Splash247 RSS feed...")
    
    try:
        feed = feedparser.parse(feed_url)
        print(f"Found {len(feed.entries)} entries from Splash247")
        
        for entry in feed.entries:
            # Extract categories
            categories = []
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags]
            elif hasattr(entry, 'categories'):
                categories = entry.categories
            
            # Clean description (remove HTML tags if present)
            description = entry.get('description', '')
            if description:
                import re
                description = re.sub('<[^<]+?>', '', description)  # Remove HTML tags
                description = description.replace('\n', ' ').strip()  # Clean whitespace
            
            article = {
                'title': entry.title,
                'link': entry.link,
                'creator': entry.get('author', ''),
                'pubdate': entry.get('published', ''),
                'category': '|'.join(categories),
                'description': description,
                'source': 'Splash247'
            }
            articles.append(article)
            
    except Exception as e:
        print(f"Error scraping Splash247: {e}")
    
    return articles

def scrape_tradewinds_html():
    """Scrape TradeWinds latest news page"""
    url = 'https://www.tradewindsnews.com/latest'
    articles = []
    
    print("Scraping TradeWinds latest news...")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find article containers - TradeWinds uses specific CSS classes
        article_containers = soup.find_all(['article', 'div'], class_=['article', 'story', 'news-item', 'content-item'])
        
        # If the above doesn't work, try finding by common patterns
        if not article_containers:
            article_containers = soup.find_all(['div'], class_=lambda x: x and ('story' in x.lower() or 'article' in x.lower() or 'news' in x.lower()))
        
        # Fallback: look for h2, h3 tags with links (common for news headlines)
        if not article_containers:
            article_containers = soup.find_all(['h2', 'h3', 'h4'])
        
        print(f"Found {len(article_containers)} potential article containers")
        
        for container in article_containers[:20]:  # Limit to first 20 articles
            try:
                # Try to find title
                title_elem = None
                link_elem = None
                
                # Look for title in various ways
                if container.name in ['h2', 'h3', 'h4']:
                    title_elem = container
                    link_elem = container.find('a')
                else:
                    title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'a'])
                    link_elem = container.find('a')
                
                if not title_elem:
                    continue
                
                title = title_elem.get_text().strip()
                if not title or len(title) < 10:  # Skip very short titles
                    continue
                
                # Get link
                link = ''
                if link_elem and link_elem.get('href'):
                    link = link_elem.get('href')
                    if link.startswith('/'):
                        link = 'https://www.tradewindsnews.com' + link
                
                # Try to find category
                category = ''
                category_elem = container.find(['span', 'div'], class_=lambda x: x and ('category' in x.lower() or 'section' in x.lower() or 'tag' in x.lower()))
                if category_elem:
                    category = category_elem.get_text().strip()
                
                # Try to find date
                pubdate = ''
                date_elem = container.find(['time', 'span', 'div'], class_=lambda x: x and ('date' in x.lower() or 'time' in x.lower()))
                if not date_elem:
                    date_elem = container.find('time')
                if date_elem:
                    pubdate = date_elem.get_text().strip()
                    if not pubdate and date_elem.get('datetime'):
                        pubdate = date_elem.get('datetime')
                
                # Try to find description
                description = ''
                desc_elem = container.find(['p', 'div'], class_=lambda x: x and ('summary' in x.lower() or 'excerpt' in x.lower() or 'description' in x.lower()))
                if not desc_elem:
                    desc_elem = container.find('p')
                if desc_elem:
                    description = desc_elem.get_text().strip()
                
                if title and len(title) > 10:  # Only add if we have a decent title
                    article = {
                        'title': title,
                        'link': link,
                        'creator': '',  # TradeWinds doesn't always show author on listing page
                        'pubdate': pubdate,
                        'category': category,
                        'description': description,
                        'source': 'TradeWinds'
                    }
                    articles.append(article)
                    print(f"Found article: {title}")
                
            except Exception as e:
                print(f"Error processing article container: {e}")
                continue
        
        print(f"Successfully scraped {len(articles)} articles from TradeWinds")
        
    except Exception as e:
        print(f"Error scraping TradeWinds: {e}")
    
    return articles

def scrape_all_sources():
    """Scrape all news sources and update CSV"""
    csv_file = 'rss_feed_articles.csv'
    
    print(f"Starting news scrape at {datetime.now()}")
    
    # Get articles from both sources
    all_articles = []
    
    # Scrape Splash247
    splash_articles = scrape_splash247_rss()
    all_articles.extend(splash_articles)
    
    # Add delay between requests to be respectful
    time.sleep(2)
    
    # Scrape TradeWinds
    tradewinds_articles = scrape_tradewinds_html()
    all_articles.extend(tradewinds_articles)
    
    print(f"Total articles found: {len(all_articles)}")
    
    # Check if CSV file exists
    file_exists = os.path.isfile(csv_file)
    
    # Get existing articles to avoid duplicates
    existing_links = set()
    if file_exists:
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_links.add(row['link'])
            print(f"Found {len(existing_links)} existing articles")
        except Exception as e:
            print(f"Error reading existing CSV: {e}")
    
    # Write to CSV
    try:
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description', 'source']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # Write headers if file is new
            if not file_exists:
                writer.writeheader()
                print("Created new CSV file with headers")
            
            # Process new entries
            new_articles_count = 0
            for article in all_articles:
                if article['link'] and article['link'] not in existing_links:
                    writer.writerow(article)
                    new_articles_count += 1
                    print(f"New article added: {article['title']} (Source: {article['source']})")
            
            print(f"Added {new_articles_count} new articles to {csv_file}")
            
    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    scrape_all_sources()
