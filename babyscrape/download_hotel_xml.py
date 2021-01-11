import urllib.robotparser as urobot
import urllib.request
import requests
import os
from bs4 import BeautifulSoup


class FetchError(Exception):
    pass

def fetch_sitemap_list(root_url):
    robots_url = root_url +'/robots.txt'
    robots_parser = urobot.RobotFileParser()
    robots_parser.set_url(robots_url)
    robots_parser.read()
    if robots_parser.can_fetch("*", robots_url):
        site = urllib.request.urlopen(robots_url)
        sauce = site.read()
        soup = BeautifulSoup(sauce, "html.parser")
        split_soup = soup.get_text().split('\n')
        sitemap_list = []
        for line in split_soup:
            if "Sitemap" in line:
                sitemap_list.append(line.replace("Sitemap: ", ""))
        return sitemap_list
    else:
        raise FetchError('Cannot Fetch Robots.txt')


def get_xml_list(site_index_url):
    xml_list = urllib.request.urlopen(site_index_url)
    xml_response = xml_list.read()
    xml_soup = BeautifulSoup(xml_response, "html.parser")
    return xml_soup.find_all('loc')


def filter_xmls(list_xmls_from_soup, filter_string):
    filtered_xmls =[]
    for item in list_xmls_from_soup:
        if filter_string in item.text:
            filtered_xmls.append(item.text)
    return filtered_xmls


def save_xml_gzs(xml_url):
    filename = xml_url.split("/")[-1]
    cwd = os.getcwd()
    path = os.path.join(cwd, 'input', filename)
    with open(path, "wb") as file:
        url_request = requests.get(xml_url)
        file.write(url_request.content)


if __name__ == "__main__":
    root_url = 'https://www.tripadvisor.ca'
    sitemap_list = fetch_sitemap_list(root_url)
    filter_key = 'CA_index'
    site_index_url = next((xml for xml in sitemap_list if filter_key in xml), None)
    list_all_xmls = get_xml_list(site_index_url)
    hotel_xmls = filter_xmls(list_all_xmls, '-hotel_review-')
    for xml_url in hotel_xmls:
        save_xml_gzs(xml_url)