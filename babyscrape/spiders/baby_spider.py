import scrapy

class BabySpider(scrapy.Spider):

    name = 'hotel'
    start_urls =[
        'https://www.tripadvisor.ca/Hotel_Review-g154913-d7093537-Reviews-Hotel_Clique_Calgary_Airport-Calgary_Alberta.html'
    ]
    readmore_clicked = False
    def parse(self, response):

        about_rating = response.css('div._1krg1t5y *::attr(class)').extract()

        metadata = {
            'metadata': {
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
                'description': response.css('div.cPQsENeY::text').extract_first()
            }
        }

        reviews_on_page = response.css('div._2wrUUKlw')
        for review_response in reviews_on_page:
            metadata['review'] = self.scrape_review(review_response)
            yield metadata

    def scrape_review(self, review_response):
        bubble_rating = review_response.css('span.ui_bubble_rating::attr(class)').extract_first()
        review = {
            'id_user': review_response.css('a::attr(href)').extract_first().replace('/Profile/', ''),
            'name_user': review_response.css('div._2fxQ4TOx *::text').extract()[0],
            'review_date': review_response.css('div._2fxQ4TOx *::text').extract()[1],
            'review_rating': self.bubble_breaker(bubble_rating),
            'reviewer_location': review_response.css('span._1TuWwpYf *::text').extract_first(),
            'review_title': review_response.css('div.glasR4aX *::text').extract_first(),
            'review_text': review_response.css('q.IRsGHoPm *::text').extract_first(),
            'review_response': review_response.css('span.sT5TMxg3 *::text').extract(),
            'review_responder': review_response.css('div._204cKjWJ::text').extract_first(),
        }
        return review

    def bubble_breaker(self, ui_bubble_rating_string):
        return ui_bubble_rating_string[-2] + '.' + ui_bubble_rating_string[-1]



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
