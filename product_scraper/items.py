# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProductScraperItem(scrapy.Item):
    title = scrapy.Field()
    upc = scrapy.Field()
    sku = scrapy.Field()
    price = scrapy.Field()
    availability = scrapy.Field()
    brand = scrapy.Field()
    category = scrapy.Field()
    short_description = scrapy.Field()
    description = scrapy.Field()
    image1 = scrapy.Field()


    
class ProductItem(scrapy.Item):
    url = scrapy.Field()
    handle = scrapy.Field()
    title = scrapy.Field()
    
    sku = scrapy.Field()
    upc = scrapy.Field()
    price = scrapy.Field()
    availability = scrapy.Field()

    brand = scrapy.Field()
    category = scrapy.Field()

    custom_fields = scrapy.Field() #list of dicts [{name: "Short Description", "value": "test"}]
    description = scrapy.Field()

    images = scrapy.Field()      # list
    variants = scrapy.Field()    # list of dicts

    resources = scrapy.Field()  # list of {"href": ..., "title": ..., "filename": ...}

    page_title = scrapy.Field()
    product_url = scrapy.Field()
    meta_description = scrapy.Field()
    
    keywords = scrapy.Field()
    search_keywords = scrapy.Field()
    meta_keywords = scrapy.Field()
