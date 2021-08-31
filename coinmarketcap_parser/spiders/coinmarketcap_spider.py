import scrapy, re
from bs4 import BeautifulSoup

class CoinmarketcapSpider(scrapy.Spider):
    name = "coinmarketcap"
    custom_settings = {
         'DOWNLOAD_DELAY': 1,
    }

    def start_requests(self):
        urls = ['https://coinmarketcap.com/']
        # Initiating output file
        output_file = open('coinmarketcap.csv', 'w')
        output_file.write('name, name_abr, price_value, market_cap, fully_distributed_market_cap, volume_24h, volume_slash_market_cap, circulating_supply \n')
        for url in urls:
            yield scrapy.Request(url, self.parse_table)

    def parse_table(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        tbody_tag = soup.find('tbody')  # Table body
        tr_tag = tbody_tag.find('tr')  # Table row
        while tr_tag is not None:
            td_tags = tr_tag.find_all('td')  # Column data
            coin_page_url = 'https://coinmarketcap.com' + td_tags[2].find('a')['href']
            yield scrapy.Request(coin_page_url, self.parse_coin)
            tr_tag = tr_tag.nextSibling
        next_page_tag = soup.find("li", class_="next")
        if next_page_tag is not None and 'disabled' not in next_page_tag['class']:
            next_page_url = response.urljoin(next_page_tag.a['href'])
            yield scrapy.Request(next_page_url, self.parse_table)

    def parse_coin(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        name_tags = soup.find('div', {'class': re.compile("nameHeader(.*)")})
        name = name_tags.find('h2').next_element.string
        name_abr = name_tags.find('small').string
        price_value = soup.find('div', {'class': re.compile("priceValue(.*)")}).string
        stats_value_tags = soup.find_all('div', {'class': re.compile("statsValue(.*)")})
        market_cap = stats_value_tags[0].string
        fully_distributed_market_cap = stats_value_tags[1].string
        volume_24h = stats_value_tags[2].string
        volume_slash_market_cap = stats_value_tags[3].string
        circulating_supply = stats_value_tags[4].string

        output_file = open('coinmarketcap.csv', 'a')
        output_file.write('"'+'","'.join([name, name_abr, price_value,
                                   market_cap, fully_distributed_market_cap, volume_24h,
                                   volume_slash_market_cap, circulating_supply]) + '"\n')
