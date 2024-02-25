from typing import Iterable, List
from urllib.parse import urlencode
import pandas as pd
import scrapy
from scrapy.http import Request, Response



class TruePeopleSearch(scrapy.Spider):
    name = "truepeoplesearch"
    remaining = None
    count = 0


    def start_requests(self) -> Iterable[Request]:
        persons = self.load_input()
        self.remaining = len(persons)
        for person in persons:
            name, url = self.build_url(person)
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"name": name})


    def parse(self, response: Response, name: str):
        """
        parse the results page
        """
        self.count +=1
        self.logger.info(f" [+] Processed: {self.count}  Remaining: {self.remaining - self.count}")

        persons = response.xpath("//div[contains(@class, 'card-summary')]//div[contains(@class, 'hidden-mobile')]/a[contains(@href, '/find/person')]")
        if persons:
            for person in persons[:1]:
                link = person.xpath("./@href").get()
                url = response.urljoin(link)
                yield scrapy.Request(url, callback=self.parse_person)
        else:
            yield dict(
                name=name,
                age=None,
                birth_year=None,
                street=None,
                city=None,
                region=None,
                zipcode=None,
            )


    def parse_person(self, response: Response):
        """ 
        parse the person profile page 
        """
        item = {
            "name": response.xpath("//h1/text()").get(),
            "age": response.xpath("//span[contains(text(), 'Born')]/text()").re_first("(?:Age\s)(\d+)"),
            "birth_year": response.xpath("//span[contains(text(), 'Born')]/text()").re_first("\d{4}"),
            "street": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='streetAddress']/text()").get(),
            "city": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='addressLocality']/text()").get(),
            "region": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='addressRegion']/text()").get(),
            "zipcode": response.xpath("//div[@itemprop='homeLocation']//span[@itemprop='postalCode']/text()").get()
        }

        phones = list(set(response.xpath("//span[@itemprop='telephone']/text()").getall()))
        for idx, phone in enumerate(phones, start=1):
            item[f"phone-{idx}"] = phone
            if idx == 5:
                break
        
        return item

    
    @staticmethod
    def load_input() -> List:
        queries = []
        df = pd.read_excel("input.xlsx")
        df_filtered = df.drop_duplicates(subset=['First_name', 'Last_name', 'City', 'State'])
        for idx, row in df_filtered.iterrows():
            query = {
                "first_name": row['First_name'],
                "last_name": row['Last_name'],
                "city": row['City'],
                "state": row['State'],
                "zipcode": ''
            }
            queries.append(query)
        return queries
    

    @staticmethod
    def build_url(query):
        name = f"{query.get('first_name')} {query.get('last_name')}".strip()
        city_state = f"{query.get('city')}, {query.get('state')}"
        zipcode = query.get("zipcode")
        params = {"name":name, "citystatezip": city_state or zipcode}
        url = f"https://www.truepeoplesearch.com/results?{urlencode(params)}"
        return (name, url)