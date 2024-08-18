import random
from urllib.parse import urljoin

import requests
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.response import open_in_browser

from helper import *

def get_proxies():
    proxy_path = config_data.get('PROXIES_FILE_PATH')
    with open(proxy_path, mode='r') as file:
        proxies = [f'http://{url.strip()}' for url in file.readlines()]
    return proxies

class AmazonSpider(scrapy.Spider):
    name = "amazon_Scope"

    skipped_list = set()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logs = {}
        self.cookies = {
            "regStatus": "registered",
            "ubid-main": "132-8992594-2079363",
            "i18n-prefs": "USD",
            "session-id": "140-7951262-2787032"
        }

    def start_requests(self):
        with open(config_data.get('INPUT_FILE_PATH'), mode='r') as f:
            for term in f.readlines():
                term = term.lower().strip()
                self.logs[term] = {}
                self.skipped_list.add(term)
                prompt = RULES.get('RULE_5_6_9').format(term).strip()
                yield scrapy.Request(
                    url=API_URL_GPT,
                    method='POST',
                    headers=GPT_HEADERS,
                    body=get_gpt_payload(prompt),
                    meta={'search_term': term},
                    callback=self.parse_rule_5_6_9,
                    dont_filter=True
                )

    def parse_rule_5_6_9(self, response, **kwargs):
        term = response.meta.get('search_term')
        self.logger.info('analyzing : ' + term + ' ===============')

        data = response.json()
        result = data.get('choices', [{}])[0].get('message', {}).get('content', '')
        if result == 'True':
            api_url = f"https://completion.amazon.com/api/2017/suggestions?limit=11&prefix={term}&suggestion-type=WIDGET&suggestion-type=KEYWORD&page-type=Search&alias=aps&site-variant=desktop&version=3&event=onkeypress&wc=&lop=en_US&last-prefix={term}&avg-ks-time=7481&fb=1&mid=ATVPDKIKX0DER&plain-mid=1&client-info=search-ui"
            yield scrapy.Request(api_url, meta={'term': term}, headers=HEADERS, cookies=self.cookies,
                                 callback=self.parse_rule_1)
        else:
            self.logs[term]['Rule 5-9'] = f"Failed: {result}"
            self.skipped_list.remove(term)
            yield {term: f"Rule 5-9 Failed: {result}"}
            self.save_logs()

    def parse_rule_1(self, response, **kwargs):
        term = response.meta.get('term')
        try:
            data = json.loads(response.body)['suggestions']
        except:
            self.logger.info("Couldn't get any suggestion.")
            self.logs[term]['Rule 1'] = "Failed: Couldn't get any suggestion."
            self.skipped_list.remove(term)
            yield {term: "Rule 1 Failed: Couldn't get any suggestion."}
            self.save_logs()
            return

        suggestions = []
        if len(data) >= 3:
            matching_suggestions = []
            for suggestion in data:
                value = suggestion.get('value', '').lower()
                suggestions.append(value)
                if value and term in value:
                    matching_suggestions.append(value)

            if len(matching_suggestions) >= 2:
                self.logger.info(f"Rule 1 passed for term {term}.")
                self.logs[term]['Rule 1'] = 'Passed'
                url = f"https://www.amazon.com/s?k={term}"
                meta = {'term': term, 'suggestions': suggestions}

                self.logger.info(f"Scrapping about {term}...")

                yield scrapy.Request(url, meta=meta, callback=self.parse_rule_2, cookies=self.cookies, headers=HEADERS)

            else:
                self.logger.info(f"Term {term} failed in Rule 1. {term} contains less than 2 in a suggestion.")
                self.logs[term]['Rule 1'] = f'Failed: {term} contains less than 2 in a suggestion.'
                self.skipped_list.remove(term)
                yield {term: f'Rule 1 Failed: {term} contains less than 2 in a suggestion.'}
                self.save_logs()
                return

        else:
            self.logger.info(f"Term {term} failed in Rule 1. Suggestions are less than 3.")
            self.logs[term]['Rule 1'] = f'Failed: {term} suggestions are less than 3.'
            self.skipped_list.remove(term)
            yield {term: f'Rule 1 Failed: {term} suggestions are less than 3.'}
            self.save_logs()

    def parse_rule_2(self, response, **kwargs):
        term = response.meta.get('term')

        if response.status != 200:
            self.logger.info(
                f"Rule 2 failed for term {term}. Status code are {response.status}.")
            self.logs[term]['Rule 2'] = f'Rule 2 failed for term {term}. Status code are {response.status}.'
            yield {term: f'Rule 2 failed for term {term}. Status code are {response.status}.'}
            self.save_logs()
            return

        suggestions = response.meta.get('suggestions')
        total_results = get_number_of_results(response.xpath('//span[contains(text(),"result")]/text()').get(''))
        if 0 < total_results <= 400:
            unique_urls = set()
            product_listing = []
            for product in response.xpath("//div[@data-cy='title-recipe' and not(contains(., 'Sponsored'))]"):
                bought_items = product.css("span.a-size-base:contains('bought')::text").get('')
                bought_value = ''
                if bought_items:
                    bought_value = bought_items.split()[0].rstrip('+')
                    bought_value = convert_abbreviated_number(bought_value)

                price = float(product.css(
                    "span.a-price > span.a-offscreen::text, span:contains('No featured offers available') + br +span.a-color-base::text").get(
                    '0').lstrip('$') or '0')

                url = product.css("h2 > a::attr(href)").get()
                if url in unique_urls:
                    continue
                unique_urls.add(url)
                product_listing.append({
                    "url": response.urljoin(url),
                    'name': product.css("h2 > a > span::text").get('').strip().lower(),
                    'bought_values': bought_value,
                    'price': price,
                    'monthly_sale': bought_value * price if bought_value and price else 0
                })

            if product_listing:
                min_sale = minimum_total_sales_of_search_group_for_results(total_results)

                self.logs[term]['Rule 2'] = 'Passed'
                self.save_logs()
                prompt = RULES.get('RULE_3').format(term, suggestions).strip()

                yield scrapy.Request(
                    url=API_URL_GPT,
                    method='POST',
                    headers=GPT_HEADERS,
                    body=get_gpt_payload(prompt),
                    meta={'term': term, 'total_results': total_results, 'min_sale': min_sale,
                          'searched_term': suggestions, 'data': {
                            'main_url': response.url,
                            'data': product_listing,
                            'next_page_url': response.css('a.s-pagination-next::attr(href)').get()
                        }},
                    callback=self.parse_rule_3_1
                )
        elif total_results > 400:
            self.logger.info(
                f"Rule 2 failed for term {term}. Total Items Results are more than 400.")
            self.logs[term]['Rule 2'] = f'Failed: {term} has more than 400 items'
            self.skipped_list.remove(term)
            yield {term: f'Rule 2 Failed: {term} has more than 400 items'}
            self.save_logs()
            return
        else:
            self.logger.info(
                f"Rule 2 failed for term {term}. No Total Results are found.")
            self.logs[term]['Rule 2'] = f'Failed: {term}, No Total Results are found'
            self.skipped_list.remove(term)
            yield {term: f'Rule 2 Failed: {term}, No Total Results are found'}
            self.save_logs()
            return

    def parse_rule_3_1(self, response, **kwargs):
        gpt_data = response.json()
        group_term = gpt_data.get('choices', [{}])[0].get('message', {}).get('content', '')
        meta = response.meta.copy()
        term = response.meta.get('term')
        data = response.meta.get('data')
        min_sale = response.meta.get('min_sale')

        if group_term:
            if ' ' in group_term:
                words = group_term.split()
                sorted_words = sorted(words, key=lambda x: len(x))
                group_term = ' '.join(sorted_words)

            for product in data['data']:
                if product['monthly_sale'] < min_sale:
                    self.logger.info(f"Term {term} - Product: {product['name']} with Sale: {product['monthly_sale']} failed in Rule 3-1.")
                    self.logs[term]['Rule 3-1'] = f'Failed: {product["name"]} with Sale: {product["monthly_sale"]}'
                    self.skipped_list.remove(term)
                    yield {term: f'Rule 3-1 Failed: {product["name"]} with Sale: {product["monthly_sale"]}'}
                    self.save_logs()
                    return

            if len(data['data']) < 2:
                self.logger.info(f"Term {term} failed in Rule 3-1 due to not enough products.")
                self.logs[term]['Rule 3-1'] = f'Failed: Less than 2 products.'
                self.skipped_list.remove(term)
                yield {term: f'Rule 3-1 Failed: Less than 2 products.'}
                self.save_logs()
                return

            prompt = RULES.get('RULE_3_2').format(term, group_term).strip()
            yield scrapy.Request(
                url=API_URL_GPT,
                method='POST',
                headers=GPT_HEADERS,
                body=get_gpt_payload(prompt),
                meta={'term': term, 'data': data},
                callback=self.parse_rule_3_2
            )
        else:
            self.logger.info(f"Term {term} failed in Rule 3-1 due to empty GPT response.")
            self.logs[term]['Rule 3-1'] = 'Failed: Empty GPT response'
            self.skipped_list.remove(term)
            yield {term: 'Rule 3-1 Failed: Empty GPT response'}
            self.save_logs()
            return

    def parse_rule_3_2(self, response, **kwargs):
        term = response.meta.get('term')
        data = response.meta.get('data')
        if not data or len(data['data']) < 2:
            self.logger.info(f"Term {term} failed in Rule 3-2 due to insufficient product data.")
            self.logs[term]['Rule 3-2'] = f'Failed: Insufficient product data'
            self.skipped_list.remove(term)
            yield {term: f'Rule 3-2 Failed: Insufficient product data'}
            self.save_logs()
            return

        try:
            total_monthly_sales = [product['monthly_sale'] for product in data['data']]
            if not total_monthly_sales:
                raise ValueError("No monthly sales data found")

            sorted_sales = sorted(total_monthly_sales, reverse=True)
            avg_sales = sum(sorted_sales[:2]) / 2
            threshold = avg_sales * 0.4

            failed_products = []
            for product in data['data']:
                if product['monthly_sale'] < threshold:
                    failed_products.append({
                        'product': product['name'],
                        'monthly_sale': product['monthly_sale']
                    })

            if failed_products:
                self.logger.info(f"Term {term} failed in Rule 3-2 due to products not meeting sales threshold.")
                self.logs[term]['Rule 3-2'] = f'Failed: {failed_products}'
                self.skipped_list.remove(term)
                yield {term: f'Rule 3-2 Failed: {failed_products}'}
                self.save_logs()
                return

            self.logger.info(f"Term {term} passed Rule 3-2.")
            self.logs[term]['Rule 3-2'] = 'Passed'
            self.skipped_list.remove(term)
            yield {term: f'Rule 3-2 Passed'}
            self.save_logs()
        except Exception as e:
            self.logger.error(f"Error in Rule 3-2 for term {term}: {e}")
            self.logs[term]['Rule 3-2'] = f'Error: {str(e)}'
            self.skipped_list.remove(term)
            yield {term: f'Rule 3-2 Error: {str(e)}'}
            self.save_logs()

if __name__ == "__main__":
    with open('inputs/config.json', 'r') as json_file:
        config_data = json.load(json_file)

    HEADERS = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "dpr": "1.5",
        "ect": "4g",
        "rtt": "200",
        "sec-ch-device-memory": "8",
        "sec-ch-dpr": "1.5",
        "sec-ch-ua": "\"Google Chrome\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-ch-ua-platform-version": "\"10.0.0\"",
        "sec-ch-viewport-width": "1442",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "viewport-width": "1442"
    }
    GPT_HEADERS = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {config_data.get("GPT_API_KEY")}'
    }
    RULES = config_data.get('RULES')
    API_URL_GPT = config_data.get('API_URL_GPT')
    BASE_URL = config_data.get('BASE_URL')

    class_for_sales_rank = run_class()
    PROXIES = get_proxies()
    settings = {
        'FEED_FORMAT': 'json',
        'FEED_URI': f"outputs/final_logs.json",
        'ROBOTSTXT_OBEY': False,
        'HTTPERROR_ALLOW_ALL': True,
        'DOWNLOAD_TIMEOUT': 1800,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
        'LOG_LEVEL': 'INFO',
        'CONCURRENT_REQUESTS': config_data.get('CONCURRENT_REQUESTS'),
        'CUSTOM_CONFIG_DATA': config_data,
        'RETRY_TIMES': config_data.get('RETRY_TIME'),
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG ': True,
        # 'ROTATING_PROXY_LIST_PATH': config_data.get('PROXIES_FILE_PATH')
    }

    if PROXIES:
        settings['PROXIES'] = PROXIES
        settings['DOWNLOADER_MIDDLEWARES'] = {
            # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
            'middlewares.handle_middleware.HandleMiddleware': 543,
            'middlewares.proxy_retry_middleware.ProxyRetryMiddleware': 543
        }

    process = CrawlerProcess(settings)
    crawler = process.create_crawler(AmazonSpider)
    process.crawl(crawler)
    process.start()
