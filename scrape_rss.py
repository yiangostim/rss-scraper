import feedparser
import csv
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from dateutil import parser as date_parser
import pytz

def standardize_date(date_string, source_name=""):
    """Convert various date formats to standardized Greece timezone format (DD/MM/YYYY HH:MM:SS)"""
    if not date_string or date_string.strip() == '':
        return ''
    
    # Target timezone - Greece (Europe/Athens)
    target_tz = pytz.timezone('Europe/Athens')
    
    try:
        # Handle different date formats from different sources
        parsed_date = None
        
        if source_name == 'TradeWinds':
            # TradeWinds format: "22 August 2025 14:31 GMT"
            try:
                # Replace 'GMT' with '+0000' for proper parsing
                date_string_clean = date_string.replace(' GMT', ' +0000')
                parsed_date = date_parser.parse(date_string_clean)
            except:
                pass
        
        # If TradeWinds parsing failed or it's another source, use general parsing
        if parsed_date is None:
            parsed_date = date_parser.parse(date_string)
        
        # Convert to target timezone
        if parsed_date.tzinfo is None:
            # If no timezone info, assume UTC
            parsed_date = pytz.UTC.localize(parsed_date)
        
        # Convert to Greece timezone
        greece_time = parsed_date.astimezone(target_tz)
        
        # Format as DD/MM/YYYY HH:MM:SS
        return greece_time.strftime('%d/%m/%Y %H:%M:%S')
        
    except Exception as e:
        print(f"Error parsing date '{date_string}' from {source_name}: {e}")
        return date_string  # Return original if parsing fails

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
            
            # Get publication date
            pubdate = ''
            if hasattr(entry, 'published'):
                pubdate = entry.published
            elif hasattr(entry, 'updated'):
                pubdate = entry.updated
            
            # Standardize the date format
            standardized_pubdate = standardize_date(pubdate, 'MarineLink')
            
            # Standardize the date format
            standardized_pubdate = standardize_date(pubdate, 'Shipping and Freight Resource')
            
            # Standardize the date format
            standardized_pubdate = standardize_date(pubdate, 'Splash247')
            
            article = {
                'title': entry.title,
                'link': entry.link,
                'creator': entry.get('author', ''),
                'pubdate': standardized_pubdate,
                'category': final_category,
                'description': description,
                'source': 'Splash247'
            }
            articles.append(article)
            
    except Exception as e:
        print(f"Error scraping Splash247: {e}")
    
    return articles

def scrape_maritime_executive_rss():
    """Scrape Maritime Executive RSS feed"""
    feed_url = 'https://maritime-executive.com/articles.rss'
    articles = []
    
    print("Scraping Maritime Executive RSS feed...")
    
    try:
        feed = feedparser.parse(feed_url)
        print(f"Found {len(feed.entries)} entries from Maritime Executive")
        
        for entry in feed.entries:
            # Extract categories
            categories = []
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags]
            elif hasattr(entry, 'categories'):
                categories = entry.categories
            
            # If we got a single string with pipes, split it
            if len(categories) == 1 and '|' in str(categories[0]):
                categories = str(categories[0]).split('|')
            
            # Clean up categories
            categories = [cat.strip() for cat in categories if cat.strip()]
            final_category = ', '.join(categories) if categories else ''
            
            # Get the full article content from RSS
            full_article = ''
            if hasattr(entry, 'content') and entry.content:
                # RSS content is in entry.content
                content_html = entry.content[0].value if isinstance(entry.content, list) else entry.content
                # Parse HTML and extract all <p> tags
                soup = BeautifulSoup(content_html, 'html.parser')
                paragraphs = soup.find_all('p')
                article_paragraphs = []
                
                for p in paragraphs:
                    p_text = p.get_text().strip()
                    if p_text and len(p_text) > 10:  # Skip very short paragraphs
                        article_paragraphs.append(p_text)
                
                full_article = ' '.join(article_paragraphs)
            
            # If no content in entry.content, try description
            if not full_article:
                description = entry.get('description', '')
                if description:
                    soup = BeautifulSoup(description, 'html.parser')
                    paragraphs = soup.find_all('p')
                    if paragraphs:
                        full_article = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
                    else:
                        # Remove HTML tags from description
                        import re
                        full_article = re.sub('<[^<]+?>', '', description).strip()
            
            # Get date from updated field (which contains the ISO format date)
            pubdate = ''
            if hasattr(entry, 'updated'):
                pubdate = entry.updated
            elif hasattr(entry, 'published'):
                pubdate = entry.published
            
            # Standardize the date format
            standardized_pubdate = standardize_date(pubdate, 'Maritime Executive')
            
            # Get author (include even if empty)
            author = entry.get('author', '')
            
            print(f"Maritime Executive article: {entry.title[:50]}... | Date: {pubdate} | Author: '{author}' | Content length: {len(full_article)}")
            
            article = {
                'title': entry.title,
                'link': entry.link,
                'creator': author,
                'pubdate': standardized_pubdate,
                'category': final_category,
                'description': full_article,  # Full article content
                'source': 'Maritime Executive'
            }
            articles.append(article)
            
    except Exception as e:
        print(f"Error scraping Maritime Executive: {e}")
    
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
                        
                        # Standardize the date format
                        standardized_pubdate = standardize_date(pubdate, 'TradeWinds')
                        
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
                            'pubdate': standardized_pubdate,
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

def scrape_shipping_freight_resource_rss():
    """Scrape Shipping and Freight Resource RSS feed"""
    feed_url = 'https://www.shippingandfreightresource.com/feed/'
    articles = []
    
    print("Scraping Shipping and Freight Resource RSS feed...")
    
    try:
        feed = feedparser.parse(feed_url)
        print(f"Found {len(feed.entries)} entries from Shipping and Freight Resource")
        
        for entry in feed.entries:
            # Extract categories
            categories = []
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags]
            elif hasattr(entry, 'categories'):
                categories = entry.categories
            
            # If we got a single string with pipes, split it
            if len(categories) == 1 and '|' in str(categories[0]):
                categories = str(categories[0]).split('|')
            
            # Clean up categories
            categories = [cat.strip() for cat in categories if cat.strip()]
            final_category = ', '.join(categories) if categories else ''
            
            # Clean description (remove HTML tags if present)
            description = entry.get('description', '')
            if description:
                import re
                description = re.sub('<[^<]+?>', '', description)  # Remove HTML tags
                description = description.replace('\n', ' ').strip()  # Clean whitespace
            
            # Get publication date
            pubdate = ''
            if hasattr(entry, 'published'):
                pubdate = entry.published
            elif hasattr(entry, 'updated'):
                pubdate = entry.updated
            
            article = {
                'title': entry.title,
                'link': entry.link,
                'creator': entry.get('author', ''),
                'pubdate': standardized_pubdate,
                'category': final_category,
                'description': description,
                'source': 'Shipping and Freight Resource'
            }
            articles.append(article)
            
    except Exception as e:
        print(f"Error scraping Shipping and Freight Resource: {e}")
    
    return articles

def scrape_marinelink_rss():
    """Scrape MarineLink RSS feed"""
    feed_url = 'https://www.marinelink.com/news/rss'
    articles = []
    
    print("Scraping MarineLink RSS feed...")
    
    try:
        feed = feedparser.parse(feed_url)
        print(f"Found {len(feed.entries)} entries from MarineLink")
        
        for entry in feed.entries:
            # Extract categories
            categories = []
            if hasattr(entry, 'tags'):
                categories = [tag.term for tag in entry.tags]
            elif hasattr(entry, 'categories'):
                categories = entry.categories
            
            # If we got a single string with pipes, split it
            if len(categories) == 1 and '|' in str(categories[0]):
                categories = str(categories[0]).split('|')
            
            # Clean up categories
            categories = [cat.strip() for cat in categories if cat.strip()]
            final_category = ', '.join(categories) if categories else ''
            
            # Clean description (remove HTML tags if present)
            description = entry.get('description', '')
            if description:
                import re
                description = re.sub('<[^<]+?>', '', description)  # Remove HTML tags
                description = description.replace('\n', ' ').strip()  # Clean whitespace
            
            # Get publication date
            pubdate = ''
            if hasattr(entry, 'published'):
                pubdate = entry.published
            elif hasattr(entry, 'updated'):
                pubdate = entry.updated
            
            article = {
                'title': entry.title,
                'link': entry.link,
                'creator': entry.get('author', ''),
                'pubdate': standardized_pubdate,
                'category': final_category,
                'description': description,
                'source': 'MarineLink'
            }
            articles.append(article)
            
    except Exception as e:
        print(f"Error scraping MarineLink: {e}")
    
    return articles

def scrape_hellenic_shipping_news_rss():
    """Scrape both Hellenic Shipping News RSS feeds and remove duplicates"""
    feeds = [
        ('https://www.hellenicshippingnews.com/tag/top-stories/feed/', 'Hellenic Shipping News - Top Stories'),
        ('https://www.hellenicshippingnews.com/category/shipping-news/dry-bulk-market/feed/', 'Hellenic Shipping News - Dry Bulk')
    ]
    
    all_articles = []
    processed_links = set()  # To avoid duplicates between feeds
    
    for feed_url, source_name in feeds:
        articles = []
        print(f"Scraping {source_name} RSS feed...")
        
        try:
            feed = feedparser.parse(feed_url)
            print(f"Found {len(feed.entries)} entries from {source_name}")
            
            for entry in feed.entries:
                # Skip if we already processed this article from another feed
                if entry.link in processed_links:
                    print(f"Duplicate found, skipping: {entry.title[:50]}...")
                    continue
                
                # Extract categories
                categories = []
                if hasattr(entry, 'tags'):
                    categories = [tag.term for tag in entry.tags]
                elif hasattr(entry, 'categories'):
                    categories = entry.categories
                
                # If we got a single string with pipes, split it
                if len(categories) == 1 and '|' in str(categories[0]):
                    categories = str(categories[0]).split('|')
                
                # Clean up categories
                categories = [cat.strip() for cat in categories if cat.strip()]
                final_category = ', '.join(categories) if categories else ''
                
                # Clean description (remove HTML tags if present)
                description = entry.get('description', '')
                if description:
                    import re
                    description = re.sub('<[^<]+?>', '', description)  # Remove HTML tags
                    description = description.replace('\n', ' ').strip()  # Clean whitespace
                
                # Get publication date
                pubdate = ''
                if hasattr(entry, 'published'):
                    pubdate = entry.published
                elif hasattr(entry, 'updated'):
                    pubdate = entry.updated
                
                # Standardize the date format
                standardized_pubdate = standardize_date(pubdate, source_name)
                
                article = {
                    'title': entry.title,
                    'link': entry.link,
                    'creator': entry.get('author', ''),
                    'pubdate': standardized_pubdate,
                    'category': final_category,
                    'description': description,
                    'source': source_name
                }
                articles.append(article)
                processed_links.add(entry.link)
                
        except Exception as e:
            print(f"Error scraping {source_name}: {e}")
        
        all_articles.extend(articles)
    
    print(f"Total unique articles from Hellenic Shipping News: {len(all_articles)}")
    return all_articles

def migrate_existing_csv():
    """Remove vessel_name and port columns from existing CSV and add scrape_timestamp"""
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
            
            # Check if we need to migrate
            header = lines[0]
            if 'vessel_name' in header or 'port' in header or 'scrape_timestamp' not in header:
                print("Migrating existing CSV to remove vessel/port columns and add scrape_timestamp...")
                
                # Read all data
                rows = []
                reader = csv.DictReader(lines)
                for row in reader:
                    # Remove unwanted columns and add scrape_timestamp if missing
                    new_row = {
                        'title': row.get('title', ''),
                        'link': row.get('link', ''),
                        'creator': row.get('creator', ''),
                        'pubdate': row.get('pubdate', ''),
                        'category': row.get('category', ''),
                        'description': row.get('description', ''),
                        'source': row.get('source', 'Unknown'),
                        'scrape_timestamp': row.get('scrape_timestamp', 'Unknown')  # Keep existing timestamp or mark as unknown
                    }
                    rows.append(new_row)
                
                # Write back with new structure
                fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description', 'source', 'scrape_timestamp']
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                    
                print("Successfully migrated existing CSV")
            else:
                print("CSV already has correct structure")
                
    except Exception as e:
        print(f"Error migrating CSV: {e}")

def scrape_all_sources():
    """Scrape all news sources and update CSV"""
    csv_file = 'rss_feed_articles.csv'
    
    # Get current timestamp for this scrape session (also standardized to Greece timezone)
    greece_tz = pytz.timezone('Europe/Athens')
    scrape_timestamp = datetime.now(greece_tz).strftime('%d/%m/%Y %H:%M:%S')
    print(f"Starting news scrape at {scrape_timestamp} (Greece time)")
    
    # First, migrate existing CSV if needed
    migrate_existing_csv()
    
    # Get articles from all sources
    all_articles = []
    
    # Scrape Splash247 RSS
    splash_articles = scrape_splash247_rss()
    all_articles.extend(splash_articles)
    time.sleep(2)  # Be respectful between requests
    
    # Scrape Maritime Executive RSS
    maritime_exec_articles = scrape_maritime_executive_rss()
    all_articles.extend(maritime_exec_articles)
    time.sleep(2)
    
    # Scrape TradeWinds HTML
    tradewinds_articles = scrape_tradewinds_html()
    all_articles.extend(tradewinds_articles)
    time.sleep(2)
    
    # Scrape Shipping and Freight Resource RSS
    shipping_freight_articles = scrape_shipping_freight_resource_rss()
    all_articles.extend(shipping_freight_articles)
    time.sleep(2)
    
    # Scrape MarineLink RSS
    marinelink_articles = scrape_marinelink_rss()
    all_articles.extend(marinelink_articles)
    time.sleep(2)
    
    # Scrape Hellenic Shipping News RSS feeds (with duplicate removal)
    hellenic_articles = scrape_hellenic_shipping_news_rss()
    all_articles.extend(hellenic_articles)
    
    print(f"Total articles found: {len(all_articles)}")
    
    # Add scrape timestamp to all articles
    for article in all_articles:
        article['scrape_timestamp'] = scrape_timestamp
    
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
            fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description', 'source', 'scrape_timestamp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # Write headers if file is new
            if not file_exists:
                writer.writeheader()
                print("Created new CSV file with headers")
            
            # Process new entries
            new_articles_count = 0
            for article in all_articles:
                if article.get('link') and article['link'] not in existing_links:
                    # Ensure all required fields are present
                    article_row = {
                        'title': article.get('title', ''),
                        'link': article.get('link', ''),
                        'creator': article.get('creator', ''),
                        'pubdate': article.get('pubdate', ''),
                        'category': article.get('category', ''),
                        'description': article.get('description', ''),
                        'source': article.get('source', ''),
                        'scrape_timestamp': article.get('scrape_timestamp', scrape_timestamp)
                    }
                    writer.writerow(article_row)
                    new_articles_count += 1
                    print(f"New article added: {article['title']} (Source: {article['source']})")
            
            print(f"Added {new_articles_count} new articles to {csv_file}")
            
    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    scrape_all_sources()
