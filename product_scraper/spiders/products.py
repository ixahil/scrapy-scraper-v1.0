from pathlib import Path
import pandas as pd

import scrapy

from product_scraper.items import ProductItem
import re

import unicodedata
import json
from parsel import Selector
# ADD at top of imports
from scrapy_playwright.page import PageMethod

test_urls = ["https://www.gordonelectricsupply.com/p/hoffman-tal46nnsba10104-enclosure-gnrl-accessory/7274157?text=hoffman"]

class Products(scrapy.Spider):
    name = "products"

    def __init__(self, input_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file

    def start_requests(self):
        df = pd.read_excel(self.input_file)
        for url in df["links"]:
            # yield scrapy.Request(
            #     url=url,
            #     callback=self.parse,
            #     errback=self.errback,
            #     meta={"playwright": True},
            # )
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.errback,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", ".main-content", timeout=20000),
                        PageMethod("wait_for_timeout", 1500),  # let spectable JS fire
                    ],
                },
                # meta={
                #     "playwright": True,
                #     "playwright_page_methods": [
                #         # Wrap finishLoading — after it runs, wait for DOM mutations to stop, THEN set flag
                #         PageMethod("add_init_script", """
                #             window._pageReady = false;
                #             const _original = window.finishLoading;
                #             window.finishLoading = function() {
                #                 if (_original) _original.apply(this, arguments);
                #                 // Observe DOM — when mutations stop for 500ms, page is settled
                #                 let timer;
                #                 const observer = new MutationObserver(() => {
                #                     clearTimeout(timer);
                #                     timer = setTimeout(() => {
                #                         observer.disconnect();
                #                         window._pageReady = true;
                #                     }, 500);
                #                 });
                #                 observer.observe(document.body, { childList: true, subtree: true });
                #                 // Fallback — if no mutations at all, mark ready after 1s
                #                 setTimeout(() => { window._pageReady = true; }, 1000);
                #             };
                #         """),
                #         PageMethod("wait_for_function", "() => window._pageReady === true", timeout=20000),
                #     ],
                # }
            )

    def errback(self, failure):
        self.logger.error(f"Failed: {failure.request.url}")
        # Push a failed item through the pipeline so it gets logged in CSV
        item = ProductItem()
        item["url"] = failure.request.url
        item["title"] = None  # triggers FAIL status in pipeline
        item["images"] = []
        item["variants"] = []
        item["custom_fields"] = []
        return item


    # def start_requests(self):
    #     df = pd.read_excel(self.input_file)
    #     for url in df["links"]:
    #         yield scrapy.Request(url=url, callback=self.parse, meta={
    #             "playwright": True,
    #             "playwright_context": "default",
    #         })

    def normalize_title(self, title):
        if not title:
            return ""
        # Normalize HOFF → Hoffman
        title = re.sub(r'^HOFF\b', 'Hoffman', title, flags=re.IGNORECASE)
        # Title case but preserve known uppercase tokens (SKU-like: all caps/digits/hyphens)
        words = title.split()
        normalized = []
        for i, word in enumerate(words):
            if i == 0:
                # Always capitalize first word properly
                normalized.append(word.capitalize())
            elif re.match(r'^[A-Z0-9][A-Z0-9\-]+$', word):
                # Looks like a SKU/model number — keep as-is
                normalized.append(word.upper())
            else:
                normalized.append(word.capitalize())
        return " ".join(normalized)


    def parse(self, response):
        self.logger.info(f"GOT RESPONSE: {response.url} status={response.status}")
        item = ProductItem()
        item['url'] = response.request.url
        product = response.css(".main-content")

        # -----------------------
        # Title + Handle
        # -----------------------
        title_raw = product.css("h1::text").get()
        title = self.normalize_title(title_raw)
        item["title"] = title
        item["handle"] = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-') if title else ""
       
        # -----------------------
        # SPEC TABLE → description + custom_fields
        # -----------------------
        specs_html = product.css("#spectable").get()
        custom_fields = []
        upc = ""
        brand = ""
        category = ""
        short_description = ""
        description = ""
        spec_table_image = None
        spec_description = ""

        if specs_html:
            sel = Selector(text=specs_html)
            rows = sel.css("table tr")

            for row in rows:
                # Skip + capture image from first tr (Full Size Image row)
                row_link = row.css("td a")
                if row_link and "Full Size Image" in (row_link.css("::text").get() or ""):
                    spec_table_image = row_link.attrib.get("href")
                    continue

                # key_raw = row.css("td:first-child b::text").get()
                # val = row.css("td:last-child::text").get()
                key_raw = row.xpath("normalize-space(td[1]//text())").get()
                val = row.xpath("normalize-space(td[2]//text())").get()
                
                if not key_raw or not val:
                    continue
                key = key_raw.strip().rstrip(":")
                val = val.replace("\xa0", " ").strip()

                if not val:
                    continue

                key_lower = key.lower()

                if "upc" in key_lower:
                    upc = val
                elif "manufacturer" in key_lower:
                    brand = val
                elif "sub-category" in key_lower or "subcategory" in key_lower or "sub category" in key_lower:
                    category = val
                    custom_fields.append({"name": key, "value": val[:250]})
                elif key_lower in ["product description", "product-description"]:
                    description = val

            # Remove Full Size Image tr from HTML, rewrite h2 → h6, wrap in container
            modified = re.sub(
                r'<tr[^>]*>.*?Full Size Image.*?</tr>',
                '',
                specs_html,
                flags=re.IGNORECASE | re.DOTALL
            )
            modified = re.sub(r'<h2>(.*?)</h2>', r'<h6>\1</h6>', modified, flags=re.IGNORECASE)
            modified = re.sub(r'<table\b', '<table class="spec-table table"', modified, flags=re.IGNORECASE)
            spec_description = modified
            
            
            # description_html = (
            #     description
            #     + '<div class="cmp-container" id="description-technical">'
            #     + modified
            #     + "</div>"
            # )

          # -----------------------
        
        # SHORT DESCRIPTION
        # -----------------------
        desc_block = product.css(".product-description, .description, [class*='desc']")
        if desc_block:
            # Find the <p> that contains "Technical Description" or "Description"
            target_p = None
            for p in desc_block.css("p"):
                text = p.xpath("string(.)").get() or ""
                if "technical description" in text.lower() or "description" in text.lower():
                    target_p = p
                    break

            if target_p:
                p_html = target_p.get()
                # Remove first <span>...</span> (the label)
                p_html = re.sub(r'<span[^>]*>.*?</span>', '', p_html, count=1, flags=re.IGNORECASE | re.DOTALL)
                # Remove the colon + <br> anywhere in remaining html (first occurrence)
                p_html = re.sub(r':\s*<br\s*/?>\s*', '', p_html, count=1, flags=re.IGNORECASE)
                inner = p_html
                inner = inner.strip()
                if inner:
                    inner = Selector(text=p_html).xpath("string(.)").get() or ""
                    inner = inner.strip()
                    short_description = inner

        if short_description:
               custom_fields.append({"name": "Short Description", "value": short_description.strip()[:250]})

        # Collect resources first
        links = response.css("#resourcetable ul li a")
        resources = []
        for a in links:
            href = a.attrib.get("href", "").strip()
            link_title = " ".join(a.xpath(".//text()").getall()).strip() or "Data Sheet"
            if href:
                resources.append({"href": href, "title": link_title})

        item["resources"] = resources  # pipeline will download + rewrite hrefs

        description_html = "".join([
            f'<p class="product-long-description">{description}</p>' if description else "",
            f'<p class="product-short-description">{short_description}</p>' if short_description else "",
            f'<div class="cmp-container" id="description-technical">{spec_description}</div>' if spec_description else "",
        ])
        item["description"] = description_html

        item["upc"] = upc
        item["brand"] = brand
        item["category"] = category
        item["custom_fields"] = custom_fields

        # -----------------------
        # SHORT DESCRIPTION
        # -----------------------
        # desc_block = product.css(".product-description, .description, [class*='desc']").get()
        # if desc_block:
        #     first_p = re.search(r'<p[^>]*>(.*?)</p>', desc_block, re.IGNORECASE | re.DOTALL)
        #     if first_p:
        #         inner = re.sub(r'<[^>]+>', '', first_p.group(1)).strip()
        #         # period_idx = inner.find('.')
        #         # candidate = inner[:period_idx + 1] if period_idx != -1 else inner
        #          # Find first period NOT followed by a digit (avoids splitting 2.15" or 5.20")
        #         m = re.search(r'\.(?!\d)', inner)
        #         period_idx = m.start() if m else -1
        #         candidate = inner[:period_idx + 1] if period_idx != -1 else inner
        #         if len(candidate) <= 255:
        #             short_description = candidate

        # if short_description:
        #     short_description = short_description.removeprefix('Technical Description:').strip()
        #     # Reject if it looks like a bare UPC / all-numeric string
        #     if not re.match(r'^\d{6,}$', short_description.replace(' ', '')):
        #         custom_fields.append({"name": "Short Description", "value": short_description[:255]})

      
        # -----------------------
        # PRICE + AVAILABILITY
        # -----------------------
        price_box = product.css(".price-box")
        price_text = price_box.get() or ""
        price_match = re.search(r'\$([0-9,]+\.[0-9]{2})', price_text)
        item["price"] = price_match.group(1).replace(',', '') if price_match else None

        availability_raw = price_box.css(".total-availability").xpath('following-sibling::text()').getall()
        availability_text = " ".join(t.strip() for t in availability_raw if t.strip())
        item["availability"] = availability_text or None

        # -----------------------
        # IMAGE — gallery first, fallback to spec table link
        # -----------------------
        # Image — skip placeholder
        PLACEHOLDER = "notavailfull.gif"
        image_url = product.css(".image-box #big-image::attr(href)").get()
        if image_url and PLACEHOLDER in image_url:
            image_url = None
        if not image_url and spec_table_image and PLACEHOLDER not in spec_table_image:
            image_url = spec_table_image
        item["images"] = [image_url] if image_url else []

        # -----------------------
        # SKU
        # -----------------------
        for p in product.css(".description-box p"):
            label = p.css("strong::text").get("").strip().lower()
            if "part" in label and "number" in label:
                all_text = "".join(p.css("::text").getall()).replace("\xa0", " ").strip()
                # Remove the label prefix case-insensitively, optional colon + whitespace
                val = re.sub(r'^part\s*number\s*:?\s*', '', all_text, flags=re.IGNORECASE).strip()
                item["sku"] = val if val else None
                break
        else:
            item["sku"] = None

        # -----------------------
        # SEO FIELDS
        # -----------------------
        item["page_title"] = f"{title} | Genesee Supply Co." if title else ""
        item["product_url"] = f"/{item.get('sku')}"
        item["meta_description"] = (
            f"{title} - {short_description}" if short_description and title
            else (title or "")
        )
        keywords = category.replace("/", ",").strip(",") if category else ""
        item["search_keywords"] = keywords
        item["meta_keywords"] = keywords

        # -----------------------
        # VARIANTS (empty default)
        # -----------------------
        item["variants"] = []

        yield item