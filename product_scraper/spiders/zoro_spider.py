from tokenize import String
import scrapy
import pandas as pd
import json
import re

from product_scraper.items import ProductItem


class ZoroSpider(scrapy.Spider):
    """
    Spider for Zoro.com JSON product API endpoints.
    Reads URLs from an Excel file (column named 'links' or first column).
    Each URL should return JSON matching zoro-product-schema.json.

    Usage:
        scrapy crawl zoro -a input_file=my_urls.xlsx
    """

    name = "zoro"
    

    # Full cookie string copied directly from Postman
    COOKIE_STRING = "ldId=8ec04ebb-e48e-4ea9-8644-20f709196fbc; _gcl_au=1.1.856018857.1775830596; _ga=GA1.1.611480472.1775830597; _fbp=fb.1.1775830598410.321003803706637201; FPID=FPID2.2.5UVuh6KBD6N%2BXJAzIwD1iTs5KtIioWTTy0TGDDvjcqc%3D.1775830597; FPAU=1.1.856018857.1775830596; _gtmeec=e30%3D; usi_id=2qp4rd_1775830601; usi_coupon_user=2qp4rd_1775830601; isAuthenticated=false; page_count=15; datadome=EcU5jldQcP8nKgfLE7wQ_FdXUG9YoqM9dpOEcTbroWGvDUU4c43pShwKxPSM_ukb39LYib_F6NFtSt6oMew4VAmIN1NP7F5x0N1HghV20Phih734PTRRL~fDmgfbQ28_; bm_sv=742BF89C70CFBB198149A5DAB9182607~YAAQD63OF9XvuGKdAQAA4k0NtB+RhXSEKJtIhi0OlDXBRd1h99OapHdqZoJxK4AtHz5geInT5QJsBA9pH1mJ9bkerYb6xZqJh+V8/HYsNjCSKLLS2W773Nrz4bvi+c8XVgJOsI9VqagqFp3fD30+DtO33xgCaeSHWiknRnab1JaaNcrjlaZ/+mJ3bmYk0VxOouQ91NaWBgVxacd6kaqslEKG/AVQamSoBSJlZP1uF5GUl9MgizDTa95GTX7JKACQ~1; _abck=8899E5537571DCFCABAFB24974C2B8CD~0~YAAQFK3OF4eA8mSdAQAAyhwHtA..."

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,gu;q=0.8,hi;q=0.7",
        "apikey": "924526ffbdad25e5923b",
        "origin": "https://www.zoro.com",
        "referer": "https://www.zoro.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "x-datadog-origin": "rum",
        "x-datadog-parent-id": "6565290133685699857",
        "x-datadog-sampling-priority": "1",
        "x-datadog-trace-id": "9955739371534867352",
        "z-flags": '{"temp-navi-use-suggest-fallback":true,"temp-paypal-sdk-update-BUY-2774":true,"temp-table-view-v2-zs-4007":false,"temp-search-redirects-zs-4188":true,"temp-copurchase-new-version-3002":"test","read-requests-to-firestore-4218":true,"write-requests-to-firestore-4218":true,"return-firestore-response-hec-4218":false,"temp-flyout-dyn-title-recs-3152":true,"temp-homepage-no-coview-recs-3172":false,"temp-atc-free-shipping-sales-status-3291":false,"exp-navi-template-all-zs-4417":1}',
        "Cookie": COOKIE_STRING,   # ← raw string, exactly like Postman
    }


    def __init__(self, input_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file

    # ------------------------------------------------------------------
    # Request generation
    # ------------------------------------------------------------------

    def start_requests(self):
        df = pd.read_excel(self.input_file)

        # Support both "links" column name (same as existing spider) or first col
        if "links" in df.columns:
            urls = df["links"].dropna().tolist()
        else:
            urls = df.iloc[:, 0].dropna().tolist()

        for url in urls:
            yield scrapy.Request(
                url=str(url).strip(),
                headers=self.HEADERS,
                callback=self.parse,
                errback=self.errback,
                dont_filter=True,
                meta={"dont_merge_cookies": True},  # ← bypass Scrapy cookie middleware
            )

    # ------------------------------------------------------------------
    # Error handler
    # ------------------------------------------------------------------

    def errback(self, failure):
        self.logger.error(f"Request failed: {failure.request.url} — {failure.value}")
        item = ProductItem()
        item["url"] = failure.request.url
        item["title"] = None   # triggers FAIL status in pipeline
        item["images"] = []
        item["variants"] = []
        item["custom_fields"] = []
        item["resources"] = []
        yield item

    # ------------------------------------------------------------------
    # Parse JSON response
    # ------------------------------------------------------------------

    def parse(self, response):
        try:
            data = response.json()
        except Exception as exc:
            self.logger.error(f"Failed to decode JSON from {response.url}: {exc}")
            item = ProductItem()
            item["url"] = response.url
            item["title"] = None
            item["images"] = []
            item["variants"] = []
            item["custom_fields"] = []
            item["resources"] = []
            yield item
            return
 
        # Unwrap "products" wrapper key or bare array
        if isinstance(data, dict) and "products" in data:
            products = data["products"]
        elif isinstance(data, list):
            products = data
        elif isinstance(data, dict):
            products = [data]
        else:
            self.logger.error(f"Unexpected JSON shape at {response.url}")
            return
 
        if not products:
            return
 
        # Each product in the array is an independent simple product — yield one item each
        for product in products:
            item = ProductItem()
            item["url"] = response.url
 
            item["title"] = product.get("title", "")
            item["handle"] = product.get("slug", "")
            item["brand"] = product.get("brand", "")
            item["sku"] = product.get("mfrNo", "")
            item["upc"] = product.get("upc")
            item["zoro"] = product.get("zoroNo")

            item["price"] = product.get("price")
            item["retail_price"] = product.get("originalPrice")
            item["price_unit"] = product.get("priceUnit")
            item["package_qty"] = product.get("packageQty")

            cat_paths = product.get("primaryCategoryPaths", [])
            item["category"] = " > ".join(
                c.get("name", "") for c in cat_paths if c.get("name")
            )
  
            attributes = product.get("attributes", [])
            item["custom_fields"] = self._build_custom_fields(attributes)

            resource_files = [
                media_item for media_item in product.get("media", [])
                if not media_item.get("type", "").lower().startswith("image/")
            ]

            resources = []
            base_path = "https://www.zoro.com/static/cms/enhanced_pdf/" 

            for idx, file in enumerate(resource_files, start=1):
                name = file.get("name", "")
                file_type = file.get("type", "")

                resources.append({
                    "title": f"Technical Guide {idx}",
                    "href": f"{base_path}{name}",
                    "type": file_type
                })

            item["resources"] = resources

            item["description"] = self._build_complete_description(attributes, product.get("description"))
 
            item["images"] = self._extract_images(product.get("media", []))
 
            
            item["page_title"] = product.get("SEOTitleTag", f"{item['title']} | Genesee Supply Co")
            
            item["meta_description"] = product.get("SEOMetaDescription")
            item["product_url"] = f"/{product.get('slug') or item['sku'] or ''}"
            keywords = item["category"].replace(" > ", ",")
            item["search_keywords"] = keywords
            item["meta_keywords"] = keywords
 
            item["variants"] = []

            item["supplier"] = product.get("supplier")
            item["lead_time"] = product.get("leadTime")

 
            yield item

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_complete_description(self, attributes: list, description):
        html = ""

        # 1. Description
        if description:
            html += f"<div class='product-description'>{description}</div>"

        # 2. Spec table
        specs_tb = self._buid_spec_table(attributes)
        html += specs_tb

        # # 3. Resources (PDFs)
        # if rsource_files:
        #     resource_items = ""

        #     for idx, file in enumerate(rsource_files, start=1):
        #         name = file.get("name", "Resource File")
        #         type = file.get("type", "application/pdf")
        #         href = f"/https://www.zoro.com/static/cms/enhanced_pdf/{name}"  # adjust if your base path differs

        #         resource_items += f"""
        #         <li class="description-product-spec-link-pdf__row" index="{idx}">
        #             <div class="description-product-spec-link-pdf__first-column">
        #                 <div class="description-product-spec-link-pdf__icon-contain">
        #                     <a class="description-product-spec-link-pdf__icon-link" href="{href}" rel="noopener noreferrer" target="_blank">
        #                         <span class="description-product-spec-link-pdf__icon" data-type="{type}"></span>
        #                         <span class="sr-only">opens in a new tab</span>
        #                     </a>
        #                 </div>
        #                 <div class="description-product-spec-link-pdf__title-contain">
        #                     <h2 class="description-product-spec-link-pdf__title">
        #                         <a href="{href}" rel="noopener noreferrer" target="_blank">
        #                             {name}
        #                             <span class="sr-only">opens in a new tab</span>
        #                         </a>
        #                     </h2>
        #                 </div>
        #             </div>
        #             <div class="description-product-spec-link-pdf__second-column">
        #                 <div class="description-product-spec-link-pdf__contain">
        #                     <a class="description-product-spec-link-pdf" href="{href}" target="_blank" title="Click here to download {name}">
        #                         View
        #                         <span class="sr-only">opens in a new tab</span>
        #                     </a>
        #                 </div>
        #             </div>
        #         </li>
        #         """

        #     resources_html = f"""
        #     <div class="cmp-container description-resource" id="description-resource">
        #         <h6>Resources</h6>
        #         <ul class="description-product-spec-link-pdf__row-contain">
        #             {resource_items}
        #         </ul>
        #     </div>
        #     """

        #     html += resources_html

        return html.strip()


    def _map_availability(self, sales_status: str) -> str:
        mapping = {
            "IN_STOCK": "available",
            "ACTIVE": "available",
            "OUT_OF_STOCK": "unavailable",
            "DISCONTINUED": "disabled",
        }
        return mapping.get(sales_status.upper(), sales_status)

    def _extract_images(self, media: list) -> list:
        images = []

        for m in media:
            m_type = (m.get("type") or "").lower()
            name = (m.get("name") or "").strip()

            # keep only actual images
            if name and m_type.startswith("image/"):
                url = f"https://www.zoro.com/static/cms/product/large/{name}"

                images.append(url)

        return images

    def _build_custom_fields(self, attributes: list):
        custom_fields = []

        # keep only attributes that have 'rank'
        ranked_attrs = [attr for attr in attributes if "rank" in attr]

        # sort by rank
        sorted_attrs = sorted(ranked_attrs, key=lambda x: x["rank"])

        # take only first 10
        top_attrs = sorted_attrs[:10]

        for attr in top_attrs:
            name = (attr.get("name") or "").strip()
            value = (attr.get("value") or "").strip()

            if name and value and len(value) <= 250:
                custom_fields.append({
                    "name": name,
                    "value": value
                })

        return custom_fields

    def _buid_spec_table(self, attributes: list):
        MAX_HTML_LEN = 20000

        rows = ""
        current_len = 0

        sorted_attrs = sorted(attributes, key=lambda x: x.get("rank", 9999))

        i = 0
        while i < len(sorted_attrs):
            cols = ""

            # first valid attribute
            name1 = value1 = ""
            while i < len(sorted_attrs):
                attr1 = sorted_attrs[i]
                i += 1

                name1 = (attr1.get("name") or "").strip()
                value1 = (attr1.get("value") or "").strip()

                if name1 and value1:
                    break

            if not name1 or not value1:
                break

            cols += f"<td>{name1}</td><td>{value1}</td>"

            # second valid attribute
            name2 = value2 = ""
            while i < len(sorted_attrs):
                attr2 = sorted_attrs[i]
                i += 1

                name2 = (attr2.get("name") or "").strip()
                value2 = (attr2.get("value") or "").strip()

                if name2 and value2:
                    break

            if name2 and value2:
                cols += f"<td>{name2}</td><td>{value2}</td>"
            else:
                cols += "<td></td><td></td>"

            row = f"<tr>{cols}</tr>"

            if current_len + len(row) > MAX_HTML_LEN:
                break

            rows += row
            current_len += len(row)

        html = f"""
        <div class="cmp-container" id="description-technical">
            <div id="spectable">
                <h6>Product Specifications</h6>
                <table class="spec-table table" cellspacing="0" width="95%" bgcolor="#fff">
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
        </div>
        """

        return html.strip()
    # def _build_fields_and_description(self, attributes: list):
    #     """
    #     Maps Zoro attributes array to:
    #       - custom_fields: list of {name, value} (max 250 chars value)
    #       - description: HTML string
    #     """
    #     custom_fields = []
    #     desc_parts = []
    #     spec_rows = []

    #     sorted_attrs = sorted(attributes, key=lambda x: x.get("rank", 0))

    #     for attr in sorted_attrs:
    #         name = (attr.get("name") or "").strip()
    #         value = (attr.get("value") or "").strip()
    #         if not name or not value:
    #             continue

    #         # Extract long description into description field
    #         if name.lower() in ("description", "product description", "long description"):
    #             desc_parts.append(
    #                 f'<p class="product-long-description">{value}</p>'
    #             )

    #         # All attributes → spec table
    #         spec_rows.append(
    #             f"<tr><td><b>{name}</b></td><td>{value}</td></tr>"
    #         )

    #         # Custom fields (truncated to 250 chars)
    #         if len(value) <= 250:
    #             custom_fields.append({"name": name, "value": value})

    #     if spec_rows:
    #         rows_html = "".join(spec_rows)
    #         spec_table = (
    #             f'<table class="spec-table table"><tbody>{rows_html}</tbody></table>'
    #         )
    #         desc_parts.append(
    #             f'<div class="cmp-container" id="description-technical">{spec_table}</div>'
    #         )

    #     return custom_fields, "".join(desc_parts)