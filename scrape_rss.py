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
            # Extract categories with multiple approaches
            categories = []
            
            # Method 1: Try tags
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags]
            # Method 2: Try categories
            elif hasattr(entry, 'categories'):
                categories = entry.categories
            
            # Debug: print categories to see what we're getting
            if categories:
                print(f"Raw categories for '{entry.title[:50]}...': {categories}")
                print(f"Type: {type(categories)}, Length: {len(categories)}")
            
            # If we got a single string with pipes, split it
            if len(categories) == 1 and '|' in str(categories[0]):
                categories = str(categories[0]).split('|')
                print(f"Split pipe-separated categories: {categories}")
            
            # Clean up categories (remove empty strings, strip whitespace)
            categories = [cat.strip() for cat in categories if cat.strip()]
            
            final_category = ', '.join(categories) if categories else ''
            print(f"Final category string: '{final_category}'")
            
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
                'category': final_category,
                'description': description,
                'source': 'Splash247'
            }
            articles.append(article)
            
    except Exception as e:
        print(f"Error scraping Splash247: {e}")
    
    return articles

def scrape_tradewinds_html():
    """Scrape TradeWinds latest news page using the correct CSS selectors"""
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
        
        # Find all article cards based on the structure you provided
        # Look for elements that contain both category links and article links
        article_cards = soup.find_all('div', class_=lambda x: x and 'card' in x.lower())
        
        if not article_cards:
            # Fallback: look for any container that has both category and article links
            article_cards = soup.find_all('div')
        
        print(f"Found {len(article_cards)} potential article containers")
        
        processed_links = set()  # To avoid duplicates
        
        for card in article_cards:
            try:
                # Find category link with class "main-category"
                category_elem = card.find('a', class_=lambda x: x and 'main-category' in x)
                category = ''
                if category_elem:
                    category = category_elem.get_text().strip()
                
                # Find article title link (should have href starting with specific path)
                title_links = card.find_all('a', class_='card-link text-reset')
                if not title_links:
                    # Fallback: any link that's not the category link
                    title_links = [link for link in card.find_all('a') if link != category_elem]
                
                for title_link in title_links:
                    if not title_link or not title_link.get('href'):
                        continue
                    
                    href = title_link.get('href')
                    if href in processed_links:
                        continue
                    
                    # Skip category links, look for article links
                    if href.startswith('/') and len(href.split('/')) > 2:
                        title = title_link.get_text().strip()
                        
                        if not title or len(title) < 10:
                            continue
                        
                        # Build full URL
                        if href.startswith('/'):
                            link = 'https://www.tradewindsnews.com' + href
                        else:
                            link = href
                        
                        # Find published date
                        pubdate = ''
                        date_elem = card.find('span', class_='published-at')
                        if date_elem:
                            # Extract just the date part, skip the "Published" prefix
                            pubdate_text = date_elem.get_text().strip()
                            if 'Published' in pubdate_text:
                                pubdate = pubdate_text.replace('Published', '').strip()
                            else:
                                pubdate = pubdate_text
                        
                        # Try to find description/summary
                        description = ''
                        # Look for any paragraph or div that might contain article summary
                        desc_candidates = card.find_all(['p', 'div'], class_=lambda x: x and ('summary' in x.lower() or 'excerpt' in x.lower() or 'description' in x.lower()))
                        if desc_candidates:
                            description = desc_candidates[0].get_text().strip()
                        
                        article = {
                            'title': title,
                            'link': link,
                            'creator': '',  # TradeWinds doesn't show author on listing page
                            'pubdate': pubdate,
                            'category': category,
                            'description': description,
                            'source': 'TradeWinds'
                        }
                        articles.append(article)
                        processed_links.add(href)
                        print(f"Found article: {title}")
                        
                        # Limit to avoid too many articles from one card
                        if len(articles) >= 20:
                            break
                
                if len(articles) >= 20:
                    break
                    
            except Exception as e:
                print(f"Error processing article card: {e}")
                continue
        
        print(f"Successfully scraped {len(articles)} articles from TradeWinds")
        
    except Exception as e:
        print(f"Error scraping TradeWinds: {e}")
    
    return articles

def migrate_existing_csv():
    """Add source column to existing CSV if it doesn't have it"""
    csv_file = 'rss_feed_articles.csv'
    
    if not os.path.isfile(csv_file):
        return
    
    # Read existing data
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return
            
            lines = content.split('\n')
            if not lines:
                return
            
            # Check if source column already exists
            header = lines[0]
            if 'source' in header:
                print("Source column already exists")
                return
            
            print("Adding source column to existing CSV...")
            
            # Read all data
            rows = []
            reader = csv.DictReader(lines)
            for row in reader:
                # Add source column - assume existing articles are from Splash247
                row['source'] = 'Splash247'
                rows.append(row)
        
        # Write back with new column
        fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description', 'source']
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            
        print("Successfully migrated existing CSV with source column")
        
    except Exception as e:
        print(f"Error migrating CSV: {e}")

def scrape_all_sources():
    """Scrape all news sources and update CSV"""
    csv_file = 'rss_feed_articles.csv'
    
    print(f"Starting news scrape at {datetime.now()}")
    
    # First, migrate existing CSV if needed
    migrate_existing_csv()
    
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
                    existing_links.add(row.get('link', ''))
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
                if article.get('link') and article['link'] not in existing_links:
                    writer.writerow(article)
                    new_articles_count += 1
                    print(f"New article added: {article['title']} (Source: {article['source']})")
            
            print(f"Added {new_articles_count} new articles to {csv_file}")
            
    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    scrape_all_sources()
