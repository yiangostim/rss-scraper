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
                'pubdate': pubdate,
                'category': final_category,
                'description': description,
                'source': 'Splash247',
                'vessel_name': '',  # Not applicable for this source
                'port': ''  # Not applicable for this source
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
            
            # Get author (include even if empty)
            author = entry.get('author', '')
            
            print(f"Maritime Executive article: {entry.title[:50]}... | Date: {pubdate} | Author: '{author}' | Content length: {len(full_article)}")
            
            article = {
                'title': entry.title,
                'link': entry.link,
                'creator': author,
                'pubdate': pubdate,
                'category': final_category,
                'description': full_article,  # Full article content
                'source': 'Maritime Executive',
                'vessel_name': '',  # Not applicable for this source
                'port': ''  # Not applicable for this source
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
                            'source': 'TradeWinds',
                            'vessel_name': '',  # Not applicable for this source
                            'port': ''  # Not applicable for this source
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

def scrape_marinetraffic_html():
    """Scrape MarineTraffic maritime news page"""
    url = 'https://www.marinetraffic.com/en/maritime-news'
    articles = []
    
    print("Scraping MarineTraffic maritime news...")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for news articles - MarineTraffic specific patterns
        # Try to find article containers
        article_containers = []
        
        # Common selectors for news articles
        selectors_to_try = [
            '.news-item',
            '.article-item',
            '.story',
            'article',
            '[class*="news"]',
            '[class*="story"]',
            '[class*="article"]'
        ]
        
        for selector in selectors_to_try:
            containers = soup.select(selector)
            if len(containers) > 5:  # Found a good number of articles
                article_containers = containers
                print(f"Found articles using selector: {selector}")
                break
        
        # If no containers found, look for links that seem like news articles
        if not article_containers:
            print("Trying fallback method - looking for news links")
            all_links = soup.find_all('a', href=True)
            news_links = []
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text().strip()
                if (('news' in href.lower() or 'article' in href.lower()) and 
                    len(text) > 15 and 
                    not any(skip_word in text.lower() for skip_word in ['click', 'read more', 'view', 'home'])):
                    news_links.append(link)
            
            article_containers = [link.parent.parent for link in news_links[:15]]  # Get parent containers
        
        print(f"Found {len(article_containers)} potential article containers on MarineTraffic")
        
        processed_links = set()
        
        for container in article_containers[:20]:  # Limit to avoid too many
            try:
                if not container:
                    continue
                
                # Look for title link
                title_link = container.find('a', href=True)
                if not title_link:
                    continue
                
                href = title_link.get('href', '')
                if not href or href in processed_links:
                    continue
                
                title = title_link.get_text().strip()
                if not title or len(title) < 15:
                    continue
                
                # Build full URL
                if href.startswith('/'):
                    link = 'https://www.marinetraffic.com' + href
                elif not href.startswith('http'):
                    link = 'https://www.marinetraffic.com/' + href
                else:
                    link = href
                
                # Try to find date
                pubdate = ''
                date_selectors = ['time', '[datetime]', '.date', '.published', '[class*="date"]', '.timestamp']
                for selector in date_selectors:
                    date_elem = container.select_one(selector)
                    if date_elem:
                        pubdate = date_elem.get('datetime') or date_elem.get_text().strip()
                        break
                
                # Try to find vessel name and port - MarineTraffic specific
                vessel_name = ''
                port = ''
                
                # Look for vessel name patterns
                vessel_patterns = ['.vessel-name', '[class*="vessel"]', '.ship-name', '[class*="ship"]']
                for pattern in vessel_patterns:
                    vessel_elem = container.select_one(pattern)
                    if vessel_elem:
                        vessel_name = vessel_elem.get_text().strip()
                        break
                
                # If no specific vessel element, look in title or description for vessel names
                if not vessel_name:
                    # Look for common vessel name patterns in title
                    import re
                    vessel_matches = re.findall(r'\b[A-Z][A-Z\s]+(?:STAR|QUEEN|KING|PRIDE|SPIRIT|GLORY|HARMONY|FREEDOM|VICTORY|PIONEER|EXPLORER|NAVIGATOR|EXPRESS)\b', title)
                    if vessel_matches:
                        vessel_name = vessel_matches[0].strip()
                
                # Look for port information
                port_patterns = ['.port-name', '[class*="port"]', '.location', '[class*="location"]']
                for pattern in port_patterns:
                    port_elem = container.select_one(pattern)
                    if port_elem:
                        port = port_elem.get_text().strip()
                        break
                
                # Try to find category
                category = ''
                category_elem = container.find(['span', 'div'], class_=lambda x: x and ('category' in x.lower() or 'tag' in x.lower()))
                if category_elem:
                    category = category_elem.get_text().strip()
                
                # Try to find description
                description = ''
                desc_selectors = ['p', '.summary', '.excerpt', '.description', '[class*="summary"]']
                for selector in desc_selectors:
                    desc_elem = container.select_one(selector)
                    if desc_elem:
                        desc_text = desc_elem.get_text().strip()
                        if len(desc_text) > 20:  # Only take substantial descriptions
                            description = desc_text
                            break
                
                article = {
                    'title': title,
                    'link': link,
                    'creator': '',
                    'pubdate': pubdate,
                    'category': category,
                    'description': description,
                    'source': 'MarineTraffic',
                    'vessel_name': vessel_name,
                    'port': port
                }
                articles.append(article)
                processed_links.add(href)
                print(f"Found MarineTraffic article: {title} | Vessel: '{vessel_name}' | Port: '{port}'")
                
            except Exception as e:
                print(f"Error processing MarineTraffic container: {e}")
                continue
        
        print(f"Successfully scraped {len(articles)} articles from MarineTraffic")
        
    except Exception as e:
        print(f"Error scraping MarineTraffic: {e}")
    
    return articles

def scrape_seatrade_html():
    """Scrape Seatrade Maritime latest news page using specific selectors"""
    url = 'https://www.seatrade-maritime.com/latest-news'
    articles = []
    
    print("Scraping Seatrade Maritime latest news...")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for the specific Seatrade structure you provided
        # Find all containers that have the ListPreview elements
        article_containers = soup.find_all('div', class_=lambda x: x and 'listpreview' in x.lower())
        
        if not article_containers:
            # Fallback: look for any container with the specific classes we need
            title_links = soup.find_all('a', class_='ListPreview-Title')
            article_containers = [link.parent.parent for link in title_links if link.parent and link.parent.parent]
        
        if not article_containers:
            # Another fallback: find containers with the elements we're looking for
            article_containers = []
            titles = soup.find_all('a', attrs={'data-testid': 'preview-default-title'})
            for title in titles:
                container = title.parent
                while container and container.name != 'article' and not (container.get('class') and any('preview' in cls.lower() for cls in container.get('class'))):
                    container = container.parent
                    if not container or container.name == 'body':
                        container = title.parent.parent.parent  # Fallback to great-grandparent
                        break
                if container:
                    article_containers.append(container)
        
        print(f"Found {len(article_containers)} potential article containers on Seatrade")
        
        processed_links = set()
        
        for container in article_containers[:20]:
            try:
                if not container:
                    continue
                
                # Find title using the specific selector you provided
                title_link = container.find('a', class_='ListPreview-Title')
                if not title_link:
                    title_link = container.find('a', attrs={'data-testid': 'preview-default-title'})
                if not title_link:
                    # Fallback
                    title_link = container.find('a', href=True)
                
                if not title_link:
                    continue
                
                href = title_link.get('href', '')
                if not href or href in processed_links:
                    continue
                
                title = title_link.get_text().strip()
                if not title or len(title) < 10:
                    continue
                
                # Build full URL
                if href.startswith('/'):
                    link = 'https://www.seatrade-maritime.com' + href
                elif not href.startswith('http'):
                    link = 'https://www.seatrade-maritime.com/' + href
                else:
                    link = href
                
                # Find category using the specific selector you provided
                category = ''
                category_elem = container.find('a', class_='Keyword_title_portsLogistics')
                if not category_elem:
                    category_elem = container.find('a', class_=lambda x: x and 'keyword' in x.lower())
                if not category_elem:
                    category_elem = container.find('a', attrs={'data-component': 'keyword'})
                
                if category_elem:
                    category = category_elem.get_text().strip()
                
                # Find date using the specific selector you provided
                pubdate = ''
                date_elem = container.find('span', class_='ListPreview-Date')
                if not date_elem:
                    date_elem = container.find('span', attrs={'data-testid': 'list-preview-date'})
                if not date_elem:
                    # Fallback date patterns
                    date_elem = container.find(['time', 'span'], class_=lambda x: x and 'date' in x.lower())
                
                if date_elem:
                    pubdate = date_elem.get_text().strip()
                
                # Find author using the specific selector you provided
                creator = ''
                author_elem = container.find('a', class_='Contributors-ContributorName')
                if not author_elem:
                    author_elem = container.find('a', attrs={'data-testid': 'contributor-name'})
                if not author_elem:
                    # Fallback author patterns
                    author_elem = container.find(['span', 'a'], class_=lambda x: x and ('author' in x.lower() or 'contributor' in x.lower()))
                
                if author_elem:
                    creator = author_elem.get_text().strip()
                
                # Try to find description
                description = ''
                desc_elem = container.find(['p', 'div'], class_=lambda x: x and ('summary' in x.lower() or 'excerpt' in x.lower() or 'description' in x.lower()))
                if not desc_elem:
                    desc_elem = container.find('p')
                if desc_elem:
                    description = desc_elem.get_text().strip()
                
                article = {
                    'title': title,
                    'link': link,
                    'creator': creator,
                    'pubdate': pubdate,
                    'category': category,
                    'description': description,
                    'source': 'Seatrade Maritime',
                    'vessel_name': '',  # Not applicable for this source
                    'port': ''  # Not applicable for this source
                }
                articles.append(article)
                processed_links.add(href)
                print(f"Found Seatrade article: {title} | Category: '{category}' | Author: '{creator}' | Date: '{pubdate}'")
                
            except Exception as e:
                print(f"Error processing Seatrade container: {e}")
                continue
        
        print(f"Successfully scraped {len(articles)} articles from Seatrade Maritime")
        
    except Exception as e:
        print(f"Error scraping Seatrade Maritime: {e}")
    
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
            if 'vessel_name' in header and 'port' in header:
                print("All new columns already exist")
                return
            
            print("Adding new columns to existing CSV...")
            
            # Read all data
            rows = []
            reader = csv.DictReader(lines)
            for row in reader:
                # Add missing columns - assume existing articles are from Splash247
                if 'source' not in row:
                    row['source'] = 'Splash247'
                if 'vessel_name' not in row:
                    row['vessel_name'] = ''
                if 'port' not in row:
                    row['port'] = ''
                rows.append(row)
        
        # Write back with new columns
        fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description', 'source', 'vessel_name', 'port']
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                # Add the new columns for existing data
                if 'vessel_name' not in row:
                    row['vessel_name'] = ''
                if 'port' not in row:
                    row['port'] = ''
            writer.writerows(rows)
            
        print("Successfully migrated existing CSV with new columns")
        
    except Exception as e:
        print(f"Error migrating CSV: {e}")

def scrape_all_sources():
    """Scrape all news sources and update CSV"""
    csv_file = 'rss_feed_articles.csv'
    
    print(f"Starting news scrape at {datetime.now()}")
    
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
    
    # Scrape MarineTraffic HTML
    marinetraffic_articles = scrape_marinetraffic_html()
    all_articles.extend(marinetraffic_articles)
    time.sleep(2)
    
    # Scrape Seatrade Maritime HTML
    seatrade_articles = scrape_seatrade_html()
    all_articles.extend(seatrade_articles)
    
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
            fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description', 'source', 'vessel_name', 'port']
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
                        'vessel_name': article.get('vessel_name', ''),
                        'port': article.get('port', '')
                    }
                    writer.writerow(article_row)
                    new_articles_count += 1
                    print(f"New article added: {article['title']} (Source: {article['source']})")
            
            print(f"Added {new_articles_count} new articles to {csv_file}")
            
    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    scrape_all_sources()
