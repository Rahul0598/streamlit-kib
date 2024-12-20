import time
from urllib.parse import urljoin

import csv
import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup


def get_base_domain(url):
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}"


def is_valid_url(url):
    """
    Check if a URL is valid for scraping.
    Returns False for mailto links, file downloads, and non-http(s) links.
    """
    excluded_starts = ("mailto:", "tel:", "ftp:", "file:", "javascript:", "#")

    excluded_extensions = (
        ".pdf",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".zip",
        ".rar",
        ".mp3",
        ".mp4",
        ".avi",
        ".mov",
        ".wmv",
    )

    excluded_patterns = (
        "news",
        "calendar",
        "hashtags",
        "askus",
        "libguides",
        "devices",
        "Student/Home/Sitemap",
    )

    included_patterns = (
        "partner",
        "partners",
        "about",
        "community",
        "collaborator",
        "collaborators",
        "sponsor",
        "sponsors",
        "member",
        "members",
        "network",
        "alliance",
    )

    if any(url.lower().startswith(pattern) for pattern in excluded_starts):
        return False

    if any(url.lower().endswith(ext) for ext in excluded_extensions):
        return False

    if not url.startswith(("http://", "https://")):
        return False

    if any(pattern in url.lower() for pattern in excluded_patterns):
        return False

    if any(pattern in url.lower() for pattern in included_patterns):
        url = url.rstrip("/")
        return url.lower().endswith(included_patterns)


def make_safe_request(url, session=None, timeout=10):
    """
    Make a safe HTTP request with proper error handling.
    Returns tuple of (response, soup) or (None, None) if request fails.
    """
    if session is None:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)

        # Skip if status code indicates an error
        if response.status_code >= 400:
            print(f"Skipping {url} - Status code: {response.status_code}")
            return None, None

        soup = BeautifulSoup(response.content, "lxml")
        time.sleep(1)
        return response, soup

    except requests.exceptions.Timeout:
        print(f"Timeout for {url}, skipping...")
        return None, None
    except requests.exceptions.RequestException as e:
        print(f"Failed to request {url}, skipping...")
        return None, None
    except Exception as e:
        print(f"Unexpected error processing {url}, skipping...")
        return None, None


def get_all_urls(base_url):
    visited_urls = set()
    urls_to_visit = [base_url]
    base_domain = get_base_domain(base_url)
    all_urls = []
    session = requests.Session()

    while urls_to_visit:
        url = urls_to_visit.pop()
        if url in visited_urls:
            continue
        visited_urls.add(url)

        response, soup = make_safe_request(url, session)
        if not soup:
            continue

        all_urls.append(url)

        # Find all links on the page
        for link in soup.find_all("a", href=True):
            full_url = link["href"]

            # Handle relative URLs
            if not full_url.startswith(("http://", "https://")):
                try:
                    full_url = urljoin(url, full_url)
                except:
                    continue

            # Skip non-web URLs and certain file types
            if not is_valid_url(full_url):
                continue

            # Only add URLs within the base domain and subdomains
            if (
                get_base_domain(full_url) == base_domain
                and full_url not in visited_urls
            ):
                urls_to_visit.append(full_url)

    return all_urls


def check_for_organization_urls(url, base_domains):
    social_media_domains = [
        ".",
        "<front>",
        "facebook.com",
        "twitter.com",
        "linkedin.com",
        "instagram.com",
        "youtube.com",
        "tiktok.com",
        "pinterest.com",
        "snapchat.com",
        "x.com",
        "google.com",
        "youtu.be",
        "zoom.us",
        "sendgrid.net",
        "goo.gl",
        # News
        "cpr.org",
        "r20.rs6.net",
        "goshennews.com",
        "indianalawblog.com",
        "indianacapitalchronicle.com",
        "link.bloomerang-mail.com",
        "washingtonpost.com",
        "philanthropynewsdigest.org",
        "nytimes.com",
        "wishtv.com",
        "neimagazine.com",
        "idsnews.com",
        "npr.org",
        "science.org",
        "reuters.com",
        "bloomberg.com",
        "businesswire.com",
        "newsdata.com",
        "publicnewsservice.org",
        "kpcnews.com",
        "nwitimes.com",
        "livescience.com",
        "1039sunnyfm.com",
        "vimeo.com",
        "flickr.com",
        "medium.com",
        "tumblr.com",
        "t.co",
        "bit.ly",
        "ow.ly",
        "buffer.com",
        "hootsuite.com",
        "mailchimp.com",
        "constantcontact.com",
        "addthis.com",
        "sharethis.com",
        "disqus.com",
        "gravatar.com",
        "wp.com",
        "wordpress.com",
        "blogger.com",
        "feedburner.com",
        "typepad.com",
        "formstack.com",
        "sharepoint.com",
        "qualtrics.com",
        "squarespace-cdn.com",
        "recruitingbypaycor.com",
        "squarespace.com",
        "office365.com",
        ".tel",
        "eventbrite.com",
        "gmail.com",
        "javascript.",
        "arcgis.com",
        "office.com",
        "paypal.com",
        "mailchi.mp",
        "JavaScript.",
        "bkstr.com",
        "instructure.com",
        "outlook.com",
        "elluciancrmrecruit.com",
        "12twenty.com",
        "monday.com",
    ]

    mentioned_domains = []

    response, soup = make_safe_request(url)
    if not soup:
        return []

    for link in soup.find_all("a", href=True):
        link_url = link["href"]

        link_domain = get_base_domain(link_url)
        if any(social in link_domain for social in social_media_domains):
            continue
        if link_domain not in base_domains.values():
            print(f"New domain detected: {link_domain}")
            base_domains[link_url] = link_domain

        if link_domain in base_domains.values() and link_domain != get_base_domain(url):
            mentioned_domains.append(link_domain)

    return mentioned_domains


def find_mentions_by_urls(websites, base_domains):
    mentions = {}

    for website in websites:
        parent_domain = get_base_domain(website)
        print(f"Scraping {website}...")
        urls = get_all_urls(website)

        if parent_domain not in mentions:
            mentions[parent_domain] = set()

        for url in urls:
            mentioned_domains = check_for_organization_urls(url, base_domains)
            for mentioned_domain in mentioned_domains:
                for original_url, domain in base_domains.items():
                    if domain == mentioned_domain:
                        mentions[parent_domain].add(get_base_domain(original_url))
                        break

    mentions = {domain: list(urls) for domain, urls in mentions.items()}
    return mentions


def find_connections(meta_file="arlington-woods-Elements.csv"):
    data = pd.read_csv(meta_file)
    websites = data["website"].dropna().tolist()
    base_domains = {website: get_base_domain(website) for website in websites}

    mentions_dict = find_mentions_by_urls(websites, base_domains)

    mentions_df = pd.DataFrame(
        {
            "Domain": mentions_dict.keys(),
            "Mentioned URLs": [list(urls) for urls in mentions_dict.values()],
        }
    )

    mentions_df.to_csv("organization_mentions_by_urls.csv", index=False)


def get_org_name_from_domain(domain):
    urls_to_try = [
        f"https://www.{domain}",
        f"https://{domain}",
        f"http://www.{domain}",
        f"http://{domain}"
    ]
    
    for url in urls_to_try:
        try:
            response = requests.get(f"https://{domain}", timeout=5)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try to find organization name in title
            title_tag = soup.title
            if title_tag and title_tag.string:
                return title_tag.string.strip()
            
            # Alternatively, look for meta tags or other cues
            meta_tag = soup.find('meta', attrs={'name': 'description'})
            if meta_tag and meta_tag.get('content'):
                return meta_tag['content'].strip()
        
        except Exception as e:
            print(f"Error fetching data for {domain}: {e}")
            continue
    return domain


def make_connections(links_file='organization_mentions_by_urls.csv'):
    # Read the organization mentions CSV
    
    links = pd.read_csv(links_file)
    meta = pd.read_csv('arlington-woods-Elements.csv')
    meta['Domain'] = meta.website.apply(lambda x: get_base_domain(x) if pd.notnull(x) else x)
    domain_to_org = dict(zip(meta['Domain'], meta['Label']))

    domain_to_label = dict(zip(meta['Domain'], meta['Label']))
    connections = []
    existing_connections = pd.read_csv('connections.csv')

    for _, row in links.iterrows():
        from_domain = row['Domain']
        mentioned_domains = eval(row['Mentioned URLs'])

        from_label = domain_to_label.get(from_domain, None)
        
        for to_domain in mentioned_domains:
            to_label = domain_to_label.get(to_domain, None)
            if to_label is None:
                to_label = get_org_name_from_domain(to_domain)
            if from_label and to_label:
                connections.append({'From': from_label, 'To': to_label, 'Direction': 'directed'})


    # Create a dataframe from the connections list
    connections_df = pd.DataFrame(connections)
    
    def normalize_connection(row):
        normalized = sorted([row['From'], row['To']])
        return pd.Series({'From': normalized[0], 'To': normalized[1], 'Direction': row['Direction']})

    existing_connections = existing_connections.apply(normalize_connection, axis=1)
    new_connections = connections_df.apply(normalize_connection, axis=1)
    
    existing_tuples = set(existing_connections[['From', 'To', 'Direction']].apply(tuple, axis=1))
    new_tuples = set(new_connections[['From', 'To', 'Direction']].apply(tuple, axis=1))
    unique_tuples = new_tuples - existing_tuples
    unique_connections = pd.DataFrame(list(unique_tuples), columns=['From', 'To', 'Direction'])
    updated_connections = pd.concat([existing_connections, unique_connections], ignore_index=True)

    updated_connections.to_csv('connections_new_2.csv', index=False)

if __name__ == "__main__":
    make_connections()
