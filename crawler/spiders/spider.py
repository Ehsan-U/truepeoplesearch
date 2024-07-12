from typing import Dict, Iterable, List
from urllib.parse import urlencode
import pandas as pd
import scrapy
from scrapy.http import Request, Response
from scrapy.utils.defer import maybe_deferred_to_future
import usaddress
from nameparser import HumanName



class TruePeopleSearch(scrapy.Spider):
    name = "people"


    def start_requests(self) -> Iterable[Request]:
        queries = self.load_input()
        for query in queries:
            address_type = "property_address" if not query.get("mailing_address") else "mailing_address"
            url = self.build_url(query, address_type=address_type)
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"query": query, "address_type": address_type})


    @staticmethod
    def build_url(query, address_type: str = None):
        if address_type:
            if address_type == "mailing_address":
                address = query.get("mailing_address")
                city_state = f"{query.get('mailing_city')}, {query.get('mailing_state')}"
                zipcode = query.get("mailing_zip")
            elif address_type == "property_address":
                address = query.get("property_address")
                city_state = f"{query.get('property_city')}, {query.get('property_state')}"
                zipcode = query.get("property_zip")
            params = {"streetaddress": address, "citystatezip": city_state or zipcode}
        else:
            city = query.get('mailing_city') or query.get("property_city")
            state = query.get('mailing_state') or query.get("property_state")
            name = f"{query.get('first_name')} {query.get('last_name')}".strip()
            zipcode = query.get("mailing_zip") or query.get("property_zip")
            city_state = f"{city}, {state}"
            params = {"name":name, "citystatezip": city_state or zipcode}

        url = f"https://www.truepeoplesearch.com/results?{urlencode(params)}"
        return url


    async def parse(self, response: Response, **kwargs):
        query = kwargs.get("query")
        address_type = kwargs.get("address_type")
        best_match = await self.find_best_match(response, address_type=address_type, query=query)
        
        if best_match is None and address_type == "mailing_address":
            address_type = "property_address"
            url = self.build_url(query, address_type=address_type)
            response = await maybe_deferred_to_future(self.crawler.engine.download(scrapy.Request(url)))
            best_match = await self.find_best_match(response, address_type=address_type, query=query)

        if not best_match:
            url = self.build_url(query)
            response = await maybe_deferred_to_future(self.crawler.engine.download(scrapy.Request(url)))
            best_match = await self.find_best_match(response, query=query)

        return best_match


    def filter_results(self, response: Response, query: Dict):
        """ filter the results based on points and return descender ordered list """
        results = []
        input_city = str(query['mailing_city'] or query['property_city']).lower().strip()
        for result in response.xpath("//div[contains(@data-detail-link, '/find/person')]"):
            score = 0
            name = HumanName(result.xpath(".//div[@class='h4']/text()").get('').lower().strip())
            cities = [c.split(",")[0].lower().strip() for c in result.xpath(".//span[contains(text(), 'Lives') or contains(text(), 'Used to live')]/following-sibling::span/text()").getall()]
            city_check = (lambda: any([c for c in cities if (input_city in c or c in input_city)]))()
            if name.first == query['first_name'] and name.last == query['last_name'] and city_check and name.middle == query['middle_name']:
                score = 85
            elif name.first == query['first_name'] and name.last == query['last_name'] and city_check:
                score = 80
            elif name.first == query['first_name'] and name.last == query['last_name']:
                score = 75
            elif name.last == query['last_name'] and city_check and name.middle == query['middle_name']:
                score = 70
            elif name.last == query['last_name'] and city_check:
                score = 65
            if score >= 65:
                link = response.urljoin(result.xpath("./@data-detail-link").get())
                results.append((score, link))
        if results:
            sorted_results = sorted(results, key=lambda x: x[0], reverse=True)
            return sorted_results[:1]
        self.logger.info("No results found within criteria")
        return []
    

    def get_part_of_address(self, address, part):
        addr = usaddress.parse(address)
        for value, key in addr:
            if part == key:
                return value


    async def find_best_match(self, response: Response, **kwargs):
        query = kwargs.get("query")
        toppers = []
        results = self.filter_results(response, kwargs.get("query"))
        for idx, (link_score, link) in enumerate(results, start=1):
            score = 0
            url = response.urljoin(link)
            resp = await maybe_deferred_to_future(self.crawler.engine.download(scrapy.Request(url)))
            item = self.parse_person(resp)
            score += self.address_match(query, item, address_type=kwargs.get("address_type"))
            score += self.name_match(query, item)
            item.pop("addresses")
            item.pop("first_name")
            item.pop("last_name")
            item.pop("middle_name")
            if query.get("mailing_address"):
                address = self.format_address(query, "mailing_address")
                updated_item = {
                    "confidence": score,
                    "first_name": query.get("first_name"),
                    "middle_name": query.get("middle_name"),
                    "last_name": query.get("last_name"),
                    "property_address": query.get("property_address"),
                    "property_city": query.get("property_city"),
                    "property_state": query.get("property_state"),
                    "mailing_address": address.split(",")[0],
                    "mailing_city": self.get_part_of_address(address, "PlaceName").replace(",",''),
                    "mailing_state": self.get_part_of_address(address, "StateName").replace(",",'')
                }
            else:
                address = self.format_address(query, "property_address")
                updated_item = {
                    "confidence": score,
                    "first_name": query.get("first_name"),
                    "middle_name": query.get("middle_name"),
                    "last_name": query.get("last_name"),
                    "property_address": address.split(",")[0],
                    "property_city": self.get_part_of_address(address, "PlaceName").replace(",",''),
                    "property_state": self.get_part_of_address(address, "StateName").replace(",",''),
                    "mailing_address": query.get("mailing_address"),
                    "mailing_city": query.get("mailing_city"),
                    "mailing_state": query.get("mailing_state"),
                }
            updated_item.update(**item)
            item = updated_item
            if score >= 75:
                toppers.append((score, item))
                break
            if idx == 3:
                break
        if not toppers:
            new_item = {"confidence": 0}
            new_item.update(**query)
            return new_item
        highest_score = (lambda: max([topper[0] for topper in toppers]))()
        for topper in toppers:
            if topper[0] == highest_score:
                self.logger.info(f"Highest Score: {score}")
                return topper[1]  # person with highest score
        


    def parse_person(self, response: Response):
        """
        parse the person profile page
        """
        name = HumanName(response.xpath("//h1/text()").get())
        addresses = self.get_addresses(response.xpath("//a[@data-link-to-more='address']"))
        item = {
            "first_name": name.first.lower().strip(),
            "middle_name": name.middle.lower().strip(),
            "last_name": name.last.lower().strip(),
            "addresses": addresses,
            "source": response.url,
            "phone-1": None,
            "phone-1-type": None,
            "phone-2": None,
            "phone-2-type": None,
            "phone-3": None,
            "phone-3-type": None,
            "phone-4": None,
            "phone-4-type": None,
            "phone-5": None,
            "phone-5-type": None,
            "phone-6": None,
            "phone-6-type": None,
            "email-1": None,
            "email-2": None,
            "email-3": None,
        }

        phones = response.xpath("//span[@itemprop='telephone']/parent::a/parent::div")
        for idx, phone in enumerate(phones, start=1):
            phone_type = phone.xpath("./span/text()").get()
            phone_number = phone.xpath("./a/span/text()").get()
            item[f"phone-{idx}-type"] = phone_type
            item[f"phone-{idx}"] = phone_number
            if idx == 6:
                break
        emails = set(response.xpath("//i[contains(@class, 'fa-envelope')]/parent::div/parent::div//div[contains(text(), '@')]/text()").getall())
        for idx, email in enumerate(emails, start=1):
            item[f"email-{idx}"] = email.strip()
            if idx == 3:
                break
        return item
    

    def get_addresses(self, els: scrapy.Selector):
        addresses = []
        for el in els:
            street = el.xpath(".//span[@itemprop='streetAddress']/text()").get()
            city = el.xpath(".//span[@itemprop='addressLocality']/text()").get()
            region = el.xpath(".//span[@itemprop='addressRegion']/text()").get()
            zipcode = el.xpath(".//span[@itemprop='postalCode']/text()").get()
            if all([street, city, region, zipcode]):
                address = f"{street}, {city}, {region}, {zipcode}"
                if not address in addresses:
                    addresses.append(address)
        return addresses
    

    @staticmethod
    def format_address(item, address_type):
        prefix = address_type.split('_')[0]
        zipcode = str(item.get(f'{prefix}_zip')).split(".")[0]
        return f"{item.get(f'{prefix}_address')}, {item.get(f'{prefix}_city')}, {item.get(f'{prefix}_state')}, {zipcode}"
        

    def address_match(self, input_item, result_item, address_type: str):
        if address_type:
            input_address = self.format_address(input_item, address_type)
        else:
            input_address = self.format_address(input_item, "mailing_address") or self.format_address(input_item, "property_address")

        addr1 = usaddress.parse(input_address)  # [("834", 'AddressNumber'), ("same", "StreetName"), ("suger", "StreetName")]
        result_current_address = usaddress.parse(result_item.get("addresses")[0])
        labels_to_match = ['AddressNumber','StreetName','PlaceName','StateName','ZipCode']

        def _match(addr1, addr2):
            score = 0
            street_matched, placename_matched = False, False
            for (i_value, i_key), (j_value, j_key) in zip(addr1, addr2):
                if (i_key in labels_to_match and j_key in labels_to_match) and (i_key == j_key):
                    if i_value.lower().strip() == j_value.lower().strip():
                        if i_key == 'StreetName':
                            if not street_matched:
                                score += 10
                                street_matched = True
                        elif i_key == 'PlaceName':
                            if not placename_matched:
                                score += 10
                                placename_matched = True
                        else:
                            score +=10
            return score
        
        score_addr = []
        current_address_score = _match(addr1, result_current_address)
        score_addr.append((current_address_score, result_current_address))
        for addr in result_item.get("addresses")[1:]:
            score = _match(addr1, usaddress.parse(addr))
            score -= 5  # bcz its previous address
            score_addr.append((score, addr))
        highest_score = (lambda: max([s_a[0] for s_a in score_addr]))()
        for s_a in score_addr:
            if highest_score == s_a[0]:
                if s_a[0] == 45 or s_a[0] == 50:
                    return s_a[0]
        return 0
    

    def name_match(self, input_item, result_item):
        score = 0
        input_first = input_item.get("first_name", '')
        result_first = result_item.get("first_name", '')
        input_middle = input_item.get("middle_name", '').strip()
        result_middle = result_item.get("middle_name", '').strip()
        input_last = input_item.get("last_name", '')
        result_last = result_item.get("last_name", '')

        if input_first == result_first and input_last == result_last:
            score += 30
        elif input_last == result_last:
            score += 20
        if input_middle and result_middle:
            if input_middle in result_middle and input_first == result_first:
                score += 10
            elif input_middle and input_middle in result_middle:
                score += 5

        return score


    @staticmethod
    def load_input() -> List:
        queries = []
        df = pd.read_excel("input.xlsx", dtype={'Mailing Zip': str, "Property Zip": str})
        df = df.dropna(subset=['First Name', 'Last Name'])
        df = df[df[['Property Address', 'Mailing Address']].notnull().any(axis=1)]
        df['Property Zip'] = df['Property Zip'].apply(lambda x: str(x).split('.')[0] if str(x) != "nan" else "")
        df['Mailing Zip'] = df['Mailing Zip'].apply(lambda x: str(x).split('.')[0] if str(x) != "nan" else "")
        df = df.fillna('')
        for idx, row in df.iterrows():
            query = {
                "first_name": row.get('First Name', '').lower().strip(),
                "last_name": row.get('Last Name', '').lower().strip(),
                "middle_name": row.get('Middle Name', '').lower().strip(),
                "property_address": row.get('Property Address'),
                "property_city": row.get('Property City'),
                "property_state": row.get('Property State'),
                "property_zip": row.get('Property Zip'),
                "mailing_address": row.get('Mailing Address'),
                "mailing_city": row.get("Mailing City"),
                "mailing_state": row.get("Mailing State"),
                "mailing_zip": row.get("Mailing Zip")
            }
            queries.append(query)
        return queries
