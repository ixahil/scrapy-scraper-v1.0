import scrapy
import pandas as pd


input_file = "F:\Genesee Supply\brands\Hoffman.xlsx"

class ProductSpider(scrapy.Spider):
    name = "product_spider"

    def start_requests(self):
        df = pd.read_excel(input_file)
        for url in df["url"]:
            yield scrapy.Request(url=url, callback=self.parse)

    def __init__(self, input_file, output_file="output.xlsx", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.rows = []  # collect parsed records

    def start_requests(self):
        df = pd.read_excel(self.input_file)  # needs openpyxl installed
        # assume your excel column is named "links"
        links = df["links"].dropna().astype(str).tolist()

        for url in links:
            yield scrapy.Request(url=url, callback=self.parse_page, meta={"source_url": url})

    def parse_page(self, response):
        # example extraction; customize selectors for your target site
        title = response.css("title::text").get(default="").strip()

        item = {
            "source_url": response.meta["source_url"],
            "final_url": response.url,
            "title": title,
            "status": response.status,
        }

        # transform/clean here
        item["title"] = item["title"].upper()  # example transform

        self.rows.append(item)
        yield item

    def closed(self, reason):
        # write all rows to excel when spider finishes
        out_df = pd.DataFrame(self.rows)
        out_df.to_excel(self.output_file, index=False)