from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.chrome.options import Options
import json
import time
import dateparser

default_args = {
    'owner': 'Mounir',
    'start_date': datetime(2024, 5, 26, 17, 0)
}


def next_page_loop(lk, liverpool_card):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(lk)

    try:
        while True:
            try:
                button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="pagination-next-button"]'))
                )
                button.click()
                time.sleep(1)
                page = bs(driver.page_source, 'html.parser')
                liverpool_card += page.select("div[data-testid='liverpool-card']")
            except (TimeoutException, ElementClickInterceptedException):
                break
    finally:
        driver.quit()


def scrape_sport(url):
    # Get sub menu
    req = requests.get(url)
    sport_pg = bs(req.content, 'html.parser')
    sub_menu = sport_pg.select('ul[class="ssrcss-1oh7j5p-StyledMenuList e14xdrat1"]')[0].find_all('li')
    sub = [s.find('a').get('href') for s in sub_menu if s.find('a')]

    # Scrape the submenu
    articles_links = []
    for u in sub:
        u = 'https://www.bbc.com' + u
        sub_page = bs(requests.get(u).content, 'html.parser')
        articles = sub_page.select('div[type="article"]')
        articles_links += ['https://www.bbc.com' + a.find('a').get('href') for a in articles]

    # Scrape articles
    articles_data = []
    for link in articles_links:
        article_page = bs(requests.get(link).content, 'html.parser')

        try:
            menu = sport_pg.select('ul[class="ssrcss-1oh7j5p-StyledMenuList e14xdrat1"] li')[0].text
        except Exception as e:
            menu = None

        try:
            submenu = article_page.select('ul[class="ssrcss-1oh7j5p-StyledMenuList e14xdrat1"] li')[1].text
        except Exception as e:
            submenu = None

        try:
            topic = article_page.select('ul[class="ssrcss-1ukn4s-StyledMenuList e14xdrat1"]')[0].find('li').text
        except Exception as e:
            topic = None

        try:
            title = article_page.find('h1').text
        except Exception as e:
            title = None

        try:
            subtitle = article_page.select('b[class="ssrcss-1xjjfut-BoldText e5tfeyi3"]')[0].text
        except Exception as e:
            subtitle = None

        try:
            date_str = article_page.find('time').get('datetime')
            date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception as e:
            date = None

        try:
            text = ''
            body = article_page.select('div[data-component="text-block"]')
            for block in body:
                text += ' '.join([p.text for p in block.find_all('p')])
        except Exception as e:
            text = None

        try:
            images = [img.get('src') for img in article_page.find_all('img')]
        except Exception as e:
            images = None

        try:
            authors = article_page.select('div[class="ssrcss-1q14p6q-LinkWithChevronContainer e8mq1e91"]')[0].text
        except Exception as e:
            authors = None

        try:
            video = article_page.find('video').get('src') if article_page.find('video') else None
        except Exception as e:
            video = None

        articles_data.append({
            'menu': menu,
            'submenu': submenu,
            'topic': topic,
            'title': title,
            'subtitle': subtitle,
            'date': date,
            'text': text,
            'images': images,
            'authors': authors,
            'video': video
        })

    return articles_data


class BBCScraper:
    def __init__(self, url) -> None:
        try:
            page = requests.get(url)
            page.raise_for_status()
            self.soup = bs(page.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Error fetching the URL: {e}")
            self.soup = None
        self.menu = []

    def get_menu(self):
        if not self.soup:
            return []
        menu_links = []
        try:
            for cat in self.soup.select('li[data-testid="mainNavigationItemStyled"]'):
                link = cat.find("a").get("href")
                if link:
                    menu_links.append(link)
            self.menu = menu_links
        except Exception as e:
            print(f"Error getting menu links: {e}")
        return menu_links

    def get_sub_menu(self, page_url):
        sub_menu = []
        try:
            page = bs(requests.get(page_url).content, 'html.parser')
            for sub_cat in page.select('li[class="sc-f116bf72-5 bnxbUg"]'):
                link = sub_cat.find("a").get("href")
                if link:
                    sub_menu.append(link)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching sub-menu URL: {e}")
        except Exception as e:
            print(f"Error getting sub-menu links: {e}")
        return sub_menu

    def get_articles(self, page_url):
        article_links = []
        try:
            page = bs(requests.get(page_url).content, 'html.parser')
            article_div = page.select("div[data-testid='edinburgh-card']")
            london_card = page.select("div[data-testid='london-card']")
            liverpool_card = page.select("div[data-testid='liverpool-card']")

            next_page_loop(page_url, liverpool_card)

            article_div = london_card + article_div + liverpool_card
            article_links = [i.find('a').get('href') for i in article_div if i.find('a')]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching articles URL: {e}")
        except Exception as e:
            print(f"Error getting article links: {e}")
        return article_links

    def get_title(self, article_soup):
        try:
            title = article_soup.find('h1').get_text()
            return title
        except Exception as e:
            print(f"Error getting title: {e}")
        return "No Title"

    def get_subtitle(self, article_soup):
        try:
            subtitle = article_soup.select('b[class="sc-7dcfb11b-0 kVRnKf"]')[0].text
            return subtitle
        except Exception as e:
            print(f"Error getting subtitle: {e}")
        return "No Subtitle"

    def get_body(self, article_soup):
        try:
            paragraphs = article_soup.select('div[data-component="text-block"] p')
            body = ' '.join([para.get_text() for para in paragraphs[1:]])
            return body
        except Exception as e:
            print(f"Error getting body: {e}")
        return "No Body"

    def get_author(self, article_soup):
        try:
            author_tag = article_soup.select('span[data-testid="byline-name"]')[0].text[3:]
            author = author_tag.get_text() if author_tag else 'No authors'
            return author
        except Exception as e:
            print(f"Error getting author: {e}")
        return "No Authors"

    def get_date(self, article_soup):
        try:
            date_tag = article_soup.find('time')
            if date_tag:
                date_text = date_tag.get_text()
                date = self.parse_date(date_text)
            else:
                date = 'No date'
            return date
        except Exception as e:
            print(f"Error getting date: {e}")
        return "No Date"

    def parse_date(self, date_text):
        try:
            # Parse the date using dateparser
            date = dateparser.parse(date_text, settings={'RELATIVE_BASE': datetime.now()})
            return date.strftime('%Y-%m-%d %H:%M:%S') if date else 'Unknown date'
        except Exception as e:
            print(f"Error parsing date: {e}")
        return date_text

    def get_images(self, article_soup):
        try:
            images = [img['src'] for img in article_soup.find_all('img') if 'src' in img.attrs]
            return images
        except Exception as e:
            print(f"Error getting images: {e}")
        return []

    def get_videos(self, article_soup):
        try:
            videos = [video['src'] for video in article_soup.find_all('video') if 'src' in video.attrs]
            return videos
        except Exception as e:
            print(f"Error getting videos: {e}")
        return []





def scrape_and_save():
    url = 'https://www.bbc.com'

    sport_data = scrape_sport(url + '/sport')

    scraper = BBCScraper(url)

    # Getting main menu links
    menu_links = scraper.get_menu()
    menu_links.pop(2)  # delete sport from the list because we will handle it later
    articles_data = []

    for m in menu_links:
        sub_menu_links = scraper.get_sub_menu(f'https://www.bbc.com{m}' if m.startswith('/') else m)
        for sub in sub_menu_links:
            article_links = scraper.get_articles(f'https://www.bbc.com{sub}' if sub.startswith('/') else sub)
            for article in article_links:
                article_url = f'https://www.bbc.com{article}' if article.startswith('/') else article
                try:
                    article_page = requests.get(article_url)
                    article_page.raise_for_status()
                    article_soup = bs(article_page.content, 'html.parser')

                    title = scraper.get_title(article_soup)
                    subtitle = scraper.get_subtitle(article_soup)
                    body = scraper.get_body(article_soup)
                    authors = scraper.get_author(article_soup)
                    date = scraper.get_date(article_soup)
                    images = scraper.get_images(article_soup)
                    videos = scraper.get_videos(article_soup)

                    articles_data.append({
                        'menu': m,
                        'submenu': sub,
                        'topic': article,
                        'text': body,
                        'title': title,
                        'subtitle': subtitle,
                        'date': date,
                        'images': images,
                        'authors': authors,
                        'videos': videos,
                        'url': article_url
                    })
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching article URL: {e}")

    data = articles_data + sport_data
    # data = sport_data

    # Write the results to a JSON file
    try:
        with open('/tmp/bbc_articles.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
        print("Articles data successfully written to 'bbc_articles.json'")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")



with DAG('scrape-data',
         default_args=default_args,
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:

        task = PythonOperator(
        task_id='scrape_data_bbc',
        python_callable=scrape_and_save
    )


