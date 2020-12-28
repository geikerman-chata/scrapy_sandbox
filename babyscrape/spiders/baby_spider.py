import scrapy
import re
from scrapy import Request
import dateparser
import datetime

class BabySpider(scrapy.Spider):

    name = 'hotel'
    start_urls =[
        'https://www.tripadvisor.ca/Hotel_Review-g154913-d181945-Reviews-Holiday_Inn_Calgary_Airport-Calgary_Alberta.html'
    ]
    readmore_clicked = False
    scrape_list = []
    page = 1
    metadata = {}

    def parse(self, response):
        about_rating = response.css('div._1krg1t5y *::attr(class)').extract()
        grades = response.css('span.oPMurIUj::text').extract()

        self.metadata = {
            'metadata': {
                'url': response.url,
                'id_hotel': self.get_hotel_id(response.url),
                'name_hotel': response.css('h1._1mTlpMC3::text').extract_first(),
                'address_hotel': response.css('span._3ErVArsu::text').extract_first(),
                'ranking_hotel': response.css('span._28eYYeHH::text').extract_first(),
                'number_of_reviews': response.css('span._3jEYFo-z::text').extract_first(),
                'amenities': response.css('div._2rdvbNSg::text').extract(),
                'total_rating': response.css('span._3cjYfwwQ::text').extract_first(),
                'location_rating': self.bubble_breaker(about_rating[1]),
                'cleanliness_rating': self.bubble_breaker(about_rating[4]),
                'service_rating': self.bubble_breaker(about_rating[7]),
                'value_rating': self.bubble_breaker(about_rating[10]),
                'good_to_know': response.css('div._2dtF3ueh::text').extract(),
                'class_rating': response.css('svg._2aZlo29m::attr(title)').extract_first().replace(" of 5 bubbles", ''),
                'popular_mention_tags': response.css('div._3oYukhTK *::text').extract()[1:],
                'description': response.css('div._2f_ruteS._1bona3Pu._2-hMril5 * ::text').extract_first(),
                'location_grade': grades[0],
                'surrounding_restaurants': grades[1],
                'surrounding_attractions': grades[2]
            }
        }

        reviews_on_page = response.css('div._2wrUUKlw')
        for review_response in reviews_on_page:
            datapack ={}
            datapack['review'] = self.scrape_review(review_response)
            self.scrape_list.append(datapack)

        return self.check_and_scrape_next_page(response)

    def check_and_scrape_next_page(self, response):
        root_url = 'https://www.tripadvisor.ca/'
        next_button_disabled = response.css('span.ui_button.nav.next.primary.disabled').extract() != []
        if next_button_disabled or self.page ==5:
            for review in self.scrape_list:
                unique_key = str(review['review']['review_date']) + '-' + str(review['review']['id_user'])
                self.metadata[unique_key] = review['review']
            yield self.metadata

        else:
            next_href = response.css('a.ui_button.nav.next.primary::attr(href)').extract_first()
            next_page = root_url + next_href
            self.readmore_clicked = False
            self.page += 1
            print("Opening Page: {}".format(self.page))
            yield Request(next_page, callback=self.parse)

    def scrape_review(self, review_response):
        def handle_empty(input):
            if review_response == '':
                return None

        bubble_rating = review_response.css('span.ui_bubble_rating::attr(class)').extract_first()

        review = {
            'id_user': review_response.css('a::attr(href)').extract_first().replace('/Profile/', ''),
            'name_user': review_response.css('div._2fxQ4TOx *::text').extract()[0],
            'review_date': self.process_date(review_response.css('div._2fxQ4TOx *::text').extract()[1]),
            'review_rating': self.bubble_breaker(bubble_rating),
            'trip_type': review_response.css('span._2bVY3aT5::text').extract_first(),
            'helpful_votes': review_response.css('span._3kbymg8R._3kbymg8R::text').extract_first(),
            'reviewer_location': review_response.css('span._1TuWwpYf *::text').extract_first(),
            'review_title': review_response.css('div.glasR4aX *::text').extract_first(),
            'review_text': review_response.css('q.IRsGHoPm *::text').extract_first(),
            'review_response': handle_empty('\n'.join(review_response.css('span.sT5TMxg3 *::text').extract())),
            'review_responder': review_response.css('div._204cKjWJ::text').extract_first()
        }

        return review

    def bubble_breaker(self, ui_bubble_rating_string):
        return ui_bubble_rating_string[-2] + '.' + ui_bubble_rating_string[-1]

    def get_hotel_id(self, url):
        found = re.search(r'[g]\d{4,}[-][d]\d{5,}', url)
        if found:
            return found.group()
        else:
            return None

    def process_date(self, scraped_review_date):
        refined = scraped_review_date.replace(' wrote a review ', '')
        new_date = dateparser.parse(refined).strftime("%m-%Y")
        return new_date

css_dict = {
        'metadata': {
            'about_div': 'div._3koVEFzz#ABOUT_TAB *',
            'name_hotel': 'h1._1mTlpMC3::text',
            'address_hotel': 'span._3ErVArsu::text',
            'ranking_hotel': 'span._28eYYeHH::text',
            'number_of_reviews': 'span._3jEYFo-z::text',
            'amenities': 'div._2rdvbNSg::text',
            'total_rating': 'span._3cjYfwwQ::text',
            'about_rating': 'div._1krg1t5y *::attr(class)',
            'good_to_know': 'div._2dtF3ueh::text',
            'class_rating': 'div._2dtF3ueh::text',
            'popular_mention_tags': 'div._3oYukhTK *::text',
            'description': 'div.cPQsENeY::text'
                    },
        'review': {
            'bubble_rating': 'span.ui_bubble_rating::attr(class)',
            'id_user': 'a::attr(href)',
            'name_user': 'div._2fxQ4TOx *::text',
                }
        }
