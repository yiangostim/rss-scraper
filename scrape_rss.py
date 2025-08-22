import feedparser
import csv
import os
from datetime import datetime

def scrape_rss_feed():
    feed_url = 'https://splash247.com/feed/'
    csv_file = 'rss_feed_articles.csv'
    
    print(f"Starting RSS scrape at {datetime.now()}")
    
    # Parse the RSS feed
    try:
        feed = feedparser.parse(feed_url)
        print(f"Successfully parsed feed. Found {len(feed.entries)} entries")
    except Exception as e:
        print(f"Error parsing feed: {e}")
        return
    
    # Check if CSV file exists to determine if we need to write headers
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
    
    # Open CSV file in append mode
    try:
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ['title', 'link', 'creator', 'pubdate', 'category', 'description']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # Write headers if file is new
            if not file_exists:
                writer.writeheader()
                print("Created new CSV file with headers")
            
            # Process new entries
            new_articles_count = 0
            for entry in feed.entries:
                if entry.link not in existing_links:
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
                    
                    # Write to CSV
                    writer.writerow({
                        'title': entry.title,
                        'link': entry.link,
                        'creator': entry.get('author', ''),
                        'pubdate': entry.get('published', ''),
                        'category': '|'.join(categories),
                        'description': description
                    })
                    new_articles_count += 1
                    print(f"New article added: {entry.title}")
            
            print(f"Added {new_articles_count} new articles to {csv_file}")
            
    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    scrape_rss_feed()
