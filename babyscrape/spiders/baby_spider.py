import scrapy
import re
from scrapy import Request
import dateparser
from datetime import datetime
from csv import writer
import time
from langdetect import detect

class NoDateExtracted(Exception):
    pass

class BabySpider(scrapy.Spider):
    name = 'hotel'
    rotate_user_agent = True
    readmore_clicked = False
    scrape_list = []
    page = 1
    metadata = {}
    status = 'Empty'
    tic = time.time()
    is_response = False
    start_urls = [
        #"https://www.tripadvisor.ca/Hotel_Review-g807293-d15763094-Reviews-AlpinLodge_Flachau-Flachau_Austrian_Alps.html"
        "https://www.tripadvisor.ca/Hotel_Review-g154913-d10276193-Reviews-Hilton_Garden_Inn_Calgary_Downtown-Calgary_Alberta.html"
        ]

    def parse(self, response):

        if not self.metadata:
            grades = response.css('span.oPMurIUj::text').extract()
            id_hotel = self.get_hotel_id(response.url)
            bubble_dict = self.handle_ratings(response)
            self.metadata = {
                str(id_hotel): {
                    'url': response.url,
                    'id_hotel': id_hotel,
                    'name_hotel': response.css('h1._1mTlpMC3::text').extract_first(),
                    'address_hotel': response.css('span._3ErVArsu::text').extract_first(),
                    'ranking_hotel': response.css('span._28eYYeHH::text').extract_first(),
                    'number_of_reviews': response.css('span._3jEYFo-z::text').extract_first(),
                    'amenities': response.css('div._2rdvbNSg::text').extract(),
                    'total_rating': response.css('span._3cjYfwwQ::text').extract_first(),
                    'location_rating': self.bubble_breaker(bubble_dict['location']),
                    'cleanliness_rating': self.bubble_breaker(bubble_dict['cleanliness']),
                    'service_rating': self.bubble_breaker(bubble_dict['service']),
                    'value_rating': self.bubble_breaker(bubble_dict['value']),
                    'good_to_know': response.css('div._2dtF3ueh::text').extract(),
                    'class_rating': self.handle_class_rating(response.css('svg._2aZlo29m::attr(title)')),
                    'popular_mention_tags': response.css('div._3oYukhTK *::text').extract()[1:],
                    'description': self.handle_description(response.css('div._2f_ruteS._1bona3Pu._2-hMril5 * ::text')),
                    'location_grade': grades[0] if grades else None,
                    'surrounding_restaurants': grades[1] if grades else None,
                    'surrounding_attractions': grades[2] if grades else None
                }
            }

        reviews_on_page = response.css('div._2wrUUKlw')
        for review_response in reviews_on_page:
            datapack = {'review': self.scrape_review(review_response)}
            self.scrape_list.append(datapack)

        return self.check_and_scrape_next_page(response)


    def check_and_scrape_next_page(self, response):
        root_url = 'https://www.tripadvisor.ca/'
        next_button_disabled = response.css('span.ui_button.nav.next.primary.disabled').extract() != []
        next_href = response.css('a.ui_button.nav.next.primary::attr(href)').extract_first()
        if next_button_disabled or next_href is None:
            for review in self.scrape_list:
                response_signal = 'Y' if review['review']['review_response'] else 'N'
                unique_key = response_signal + '-' + str(review['review']['review_date']) + '-' \
                             + str(review['review']['id_user'])
                self.metadata[unique_key] = review['review']
            self.status = 'Success'
            yield self.metadata

        else:
            next_page = root_url + next_href
            self.readmore_clicked = False
            self.page += 1
            print("Opening Page: {}".format(self.page))
            yield Request(next_page, callback=self.parse)

    def scrape_review(self, review_response):
        def handle_empty(input):
            if input == '':
                return None
            else:
                return input
        responder = self.handle_responder(review_response.css('div._204cKjWJ::text'))
        bubble_rating = review_response.css('span.ui_bubble_rating::attr(class)').extract_first()
        review = {
            'id_user': review_response.css('a::attr(href)').extract_first().replace('/Profile/', ''),
            'name_user': review_response.css('div._2fxQ4TOx *::text').extract()[0],
            'review_date': self.handle_review_date(review_response),
            'review_rating': self.bubble_breaker(bubble_rating),
            'review_language': self.check_language(review_response),
            'trip_type': review_response.css('span._2bVY3aT5::text').extract_first(),
            'helpful_votes': review_response.css('span._3kbymg8R._3kbymg8R::text').extract_first(),
            'reviewer_location': review_response.css('span._1TuWwpYf *::text').extract_first(),
            'review_title': review_response.css('div.glasR4aX *::text').extract_first(),
            'review_text': '\n'.join(review_response.css('q.IRsGHoPm *::text').extract()),
            'review_response': handle_empty('\n'.join(review_response.css('span.sT5TMxg3 *::text').extract())),
            'responder': responder[0],
            'responder_title': responder[1]
        }
        return review

    def check_language(self, review_response):
        try:
            lang = detect('\n'.join(review_response.css('q.IRsGHoPm *::text').extract()))
        except:
            lang = None
        return lang

    def try_grab_date(self, review_response):
        scraped_review_date_try = review_response.css('div._2fxQ4TOx *::text').extract()
        scraped_date_of_stay_try = review_response.css('span._34Xs-BQm *::text').extract()
        if len(scraped_review_date_try) == 2:
            scraped_review_date = scraped_review_date_try[1]
            scraped_date_of_stay = None
        elif len(scraped_review_date_try) == 2:
            scraped_date_of_stay = scraped_date_of_stay_try[1]
            scraped_review_date = None
        else:
            scraped_date_of_stay = None
            scraped_review_date = None
        return scraped_review_date, scraped_date_of_stay

    def handle_review_date(self, review_response):
        scraped_review_date, scraped_date_of_stay = self.try_grab_date(review_response)
        if not scraped_review_date and not scraped_date_of_stay:
            time.sleep(2)
            scraped_review_date, scraped_date_of_stay = self.try_grab_date(review_response)
        if scraped_review_date:
            refined = scraped_review_date.replace(' wrote a review ', '')
            parsed = dateparser.parse(refined)
            if parsed:
                review_date = parsed.strftime("%m-%Y")
            else:
                review_date = '00-0000'
        elif scraped_date_of_stay:
            parsed = dateparser.parse(scraped_date_of_stay)
            if parsed:
                review_date = parsed.strftime("%m-%Y")
            else:
                review_date = '00-0000'
        else:
            review_date = '00-0000'
        return review_date

    def handle_ratings(self, response):
        bubbles = response.css('div._1krg1t5y *::attr(class)').extract()
        bubble_texts = response.css('div._1krg1t5y *::text').extract()
        bubble_idx = [1, 4, 7, 10]
        bubble_dict = {
                'location': None,
                'cleanliness': None,
                'service': None,
                'value': None
            }
        for index, text in enumerate(bubble_texts):
            for option in bubble_dict.keys():
                if option.lower() in text.lower():
                    bubble_dict[option] = bubbles[bubble_idx[index]]
        return bubble_dict

    def handle_responder(self, element):
        if element.extract_first():
            responder_full = element.extract_first()
            if len(responder_full.split(',')) == 2:
                responder = responder_full.split(',')[0].replace('Response from ', '')
                title = responder_full.split(',')[1].split(' at')[0]
                self.is_response = True
                return [responder, title]
            else:
                return [responder_full, None]
        else:
            return [None, None]

    def handle_description(self, element):
        if element.extract():
            return '\n'.join(element.extract())
        else:
            return None

    def handle_class_rating(self, element):
        if element:
            return element.extract_first().replace(" of 5 bubbles", '')
        else:
            return None

    def handle_popular_mentions(self, element):
        if element:
            return element.extract()[1:]
        else:
            return None

    def bubble_breaker(self, ui_bubble_rating_string):
        if ui_bubble_rating_string:
            return ui_bubble_rating_string[-2] + '.' + ui_bubble_rating_string[-1]
        else:
            return None

    def get_hotel_id(self, url):
        found = re.search(r'[g]\d{4,}[-][d]\d{5,}', url)
        if found:
            return found.group()
        else:
            return None

    def process_date(self, scraped_review_date):
        refined = scraped_review_date.replace(' wrote a review ', '')
        try:
            new_date = dateparser.parse(refined).strftime("%m-%Y")
            return new_date
        except:
            return scraped_review_date

    def response_is_ban(self, request, response):
        return b'banned' in response.body

    def exception_is_ban(self, request, exception):
        return None

    def closed(self, reason):
        finish_time = datetime.now()
        toc = time.time()
        runtime = round(toc - self.tic, 3)
        row = [self.get_hotel_id(self.start_urls[0]), self.status, len(self.scrape_list), runtime, self.start_urls[0],
               reason, finish_time]
        with open('output/output_meta.csv', 'a+', newline='') as write_obj:
            csv_writer = writer(write_obj)
            csv_writer.writerow(row)

