import scrapy
import re
from scrapy import Request
class BabySpider(scrapy.Spider):

    name = 'hotel'
    start_urls =[
        'https://www.tripadvisor.ca/Hotel_Review-g34439-d1449858-Reviews-The_Local_House-Miami_Beach_Florida.html'
    ]
    readmore_clicked = False
    scrape_list = []
    page = 1
    def parse(self, response):
        about_rating = response.css('div._1krg1t5y *::attr(class)').extract()
        grades = response.css('span.oPMurIUj::text').extract()

        metadata = {
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
                'class_rating': response.css('svg._2aZlo29m::attr(title)').extract_first(),
                'popular_mention_tags': response.css('div._3oYukhTK *::text').extract()[1:],
                'description': response.css('div._2f_ruteS._1bona3Pu._2-hMril5 * ::text').extract_first(),
                'location_grade': grades[0],
                'surrounding_restaurants': grades[1],
                'surrounding_attractions': grades[2]
            }
        }

        reviews_on_page = response.css('div._2wrUUKlw')
        for review_response in reviews_on_page:
            metadata['review'] = self.scrape_review(review_response)
            self.scrape_list.append(self.value_check(metadata))
        return self.check_and_scrape_next_page(response)

    def check_and_scrape_next_page(self, response):
        root_url = 'https://www.tripadvisor.ca/'
        next_button_disabled = response.css('span.ui_button.nav.next.primary.disabled').extract() != []
        if next_button_disabled:
            for review in self.scrape_list:
                yield review
        else:
            next_href = response.css('a.ui_button.nav.next.primary::attr(href)').extract_first()
            next_page = root_url + next_href
            self.readmore_clicked = False
            self.page += 1
            print("Opening Page: {}".format(self.page))
            yield Request(next_page, callback=self.parse)

    def yield_out(self, output):
        yield output

    def scrape_review(self, review_response):
        bubble_rating = review_response.css('span.ui_bubble_rating::attr(class)').extract_first()
        review = {
            'id_user': review_response.css('a::attr(href)').extract_first().replace('/Profile/', ''),
            'name_user': review_response.css('div._2fxQ4TOx *::text').extract()[0],
            'review_date': review_response.css('div._2fxQ4TOx *::text').extract()[1],
            'review_rating': self.bubble_breaker(bubble_rating),
            'trip_type': review_response.css('span._2bVY3aT5::text').extract_first(),
            'helpful_votes': review_response.css('span._3kbymg8R._3kbymg8R::text').extract_first(),
            'reviewer_location': review_response.css('span._1TuWwpYf *::text').extract_first(),
            'review_title': review_response.css('div.glasR4aX *::text').extract_first(),
            'review_text': review_response.css('q.IRsGHoPm *::text').extract_first(),
            'review_response': '\n'.join(review_response.css('span.sT5TMxg3 *::text').extract()),
            'review_responder': review_response.css('div._204cKjWJ::text').extract_first()
        }

        return self.value_check(review)

    def bubble_breaker(self, ui_bubble_rating_string):
        return ui_bubble_rating_string[-2] + '.' + ui_bubble_rating_string[-1]

    def get_hotel_id(self, url):
        found = re.search(r'[g]\d{4,}[-][d]\d{5,}', url)
        if found:
            return found.group()
        else:
            return None

    def value_check(self, in_dict):
        for key in in_dict:
            if in_dict[key] == '':
                in_dict[key] = None
        return in_dict

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
            'read_more_xpath': '/html/body/div[2]/div[2]/div[2]/div[7]/div/div[1]/div[1]/div/div/div[3]/div[3]/div['
                               '2]/div[3]/div[1]/div[2]/div/span[1]',
            'bubble_rating': 'span.ui_bubble_rating::attr(class)',
            'id_user': 'a::attr(href)',
            'name_user': 'div._2fxQ4TOx *::text',
                }
        }
