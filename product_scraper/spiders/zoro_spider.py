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
          

            item["price"] = product.get("price")
            item["retail_price"] = product.get("originalPrice")
            item["price_unit"] = product.get("priceUnit")
            item["package_qty"] = product.get("packageQty")

            cat_paths = product.get("primaryCategoryPaths", [])
            item["category"] = " > ".join(
                c.get("name", "") for c in cat_paths if c.get("name")
            )
  
            attributes = product.get("attributes", [])
            item["custom_fields"], item["description"] = self._build_fields_and_description(
                attributes
            )
 
            item["images"] = self._extract_images(product.get("media", []))
 
            title = item["title"]
            item["page_title"] = product.get(
                "SEOTitleTag", f"{title} | Genesee Supply Co." if title else ""
            )
            item["meta_description"] = product.get("SEOMetaDescription", "")
            item["product_url"] = f"/{product.get('slug') or item['sku'] or ''}"
            keywords = item["category"].replace(" > ", ",")
            item["search_keywords"] = keywords
            item["meta_keywords"] = keywords
 
            item["resources"] = []
            item["variants"] = []
 
            yield item

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _map_availability(self, sales_status: str) -> str:
        mapping = {
            "IN_STOCK": "available",
            "ACTIVE": "available",
            "OUT_OF_STOCK": "unavailable",
            "DISCONTINUED": "disabled",
        }
        return mapping.get(sales_status.upper(), sales_status)

    def _extract_images(self, media: list) -> list:
        image_types = {"image", "img", "photo", "primary_image"}
        images = []
        for m in media:
            m_type = (m.get("type") or "").lower()
            name = (m.get("name") or "").strip()
            if name and (not m_type or m_type in image_types):
                images.append(name)
        return 
        
    def _build_custom_fields(self, attributes:list):
        custom_fields = []
        

    def _build_fields_and_description(self, attributes: list):
        """
        Maps Zoro attributes array to:
          - custom_fields: list of {name, value} (max 250 chars value)
          - description: HTML string
        """
        custom_fields = []
        desc_parts = []
        spec_rows = []

        sorted_attrs = sorted(attributes, key=lambda x: x.get("rank", 0))

        for attr in sorted_attrs:
            name = (attr.get("name") or "").strip()
            value = (attr.get("value") or "").strip()
            if not name or not value:
                continue

            # Extract long description into description field
            if name.lower() in ("description", "product description", "long description"):
                desc_parts.append(
                    f'<p class="product-long-description">{value}</p>'
                )

            # All attributes → spec table
            spec_rows.append(
                f"<tr><td><b>{name}</b></td><td>{value}</td></tr>"
            )

            # Custom fields (truncated to 250 chars)
            if len(value) <= 250:
                custom_fields.append({"name": name, "value": value})

        if spec_rows:
            rows_html = "".join(spec_rows)
            spec_table = (
                f'<table class="spec-table table"><tbody>{rows_html}</tbody></table>'
            )
            desc_parts.append(
                f'<div class="cmp-container" id="description-technical">{spec_table}</div>'
            )

        return custom_fields, "".join(desc_parts)

    def _build_variants(self, products: list) -> list:
        """
        Build a list of variant dicts from the products array.
        Each product in the array represents one variant.
        Only returns variants when there are 2+ distinct products.

        Each variant dict:
            {
                "sku": str,
                "price": float | None,
                "options": [{"name": str, "value": str}, ...],
                "image_url": str,
            }
        """
        if len(products) <= 1:
            return []

        variants = []
        for product in products:
            variant_sku = product.get("mfrNo") or product.get("zoroNo") or product.get("erpId", "")
            variant_price = product.get("price")

            # Build options from variantAttributes (each item describes THIS variant's options)
            options = self._extract_variant_options(
                product.get("variantAttributes", []),
                product.get("variantLabel", ""),
            )

            # Variant-level image (first image from this variant's media)
            variant_images = self._extract_images(product.get("media", []))
            variant_image = variant_images[0] if variant_images else ""

            variants.append({
                "sku": variant_sku,
                "price": variant_price,
                "options": options,
                "image_url": variant_image,
            })

        return variants

    def _extract_variant_options(self, variant_attributes: list, variant_label: str) -> list:
        """
        Converts variantAttributes into a list of {name, value} option dicts.

        variantAttributes can be:
          - list of {name, value, ...} dicts  →  use directly
          - list of arbitrary dicts with unknown keys  →  map key → value
        Falls back to variantLabel if empty.
        """
        options = []

        for va in variant_attributes:
            if not isinstance(va, dict):
                continue

            name = (va.get("name") or va.get("label") or "").strip()
            value = (va.get("value") or va.get("displayValue") or "").strip()

            if name and value:
                options.append({"name": name, "value": value})
            else:
                # Generic key→value mapping, skip internal/id fields
                skip_keys = {
                    "name", "value", "rank", "normalizedValue",
                    "mfrNo", "sku", "zoroNo", "erpId", "price",
                    "salesStatus", "media", "leadTime",
                }
                for k, v in va.items():
                    if k in skip_keys:
                        continue
                    if isinstance(v, str) and v.strip():
                        label = variant_label if variant_label else k
                        options.append({"name": label, "value": v.strip()})
                        break  # one option per attribute object

        return options