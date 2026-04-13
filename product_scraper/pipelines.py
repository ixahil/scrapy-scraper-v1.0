import pandas as pd
import os
import json
import csv
from tqdm import tqdm
import logging
from urllib.parse import urlparse

# logging.getLogger("scrapy").setLevel(logging.ERROR)
# logging.getLogger("scrapy-playwright").setLevel(logging.ERROR)
# logging.getLogger("playwright").setLevel(logging.ERROR)

COLUMNS = [
    "Status", "URL", "Item", "Name", "Type", "SKU", "Options",
    "Inventory Tracking", "Current Stock", "Low Stock", "Price",
    "Cost Price", "Retail Price", "Sale Price", "Brand ID", "Channels",
    "Categories Names","Categories", "Description", "Custom Fields", "Availability",
    "Page Title", "Product URL", "Meta Description", "Search Keywords",
    "Meta Keywords", "Bin Picking Number", "UPC/EAN", "Global Trade Number",
    "Manufacturer Part Number", "Free Shipping", "Fixed Shipping Cost",
    "Weight", "Width", "Height", "Depth", "Is Visible", "Is Featured",
    "Warranty", "Tax Class", "Product Condition", "Show Product Condition",
    "Sort Order", "Variant Image URL", "Internal Image URL (Export)",
    "Image URL (Import)", "Image Description", "Image is Thumbnail",
    "Image Sort Order", "YouTube ID", "Video Title", "Video Description",
    "Video Sort Order",
]

MAX_FILE_BYTES = 19 * 1024 * 1024  # 20 MB

# MAX_FILE_BYTES = 500 * 1024  # 500kb



class BigcommercePipeline:

    def _make_output_path(self, index: int) -> str:
        suffix = f"-part{index}" if index > 1 else ""
        return os.path.join(self._output_dir, f"{self._base_name}{suffix}-import.csv")

    def _open_csv(self, index: int):
        self.output_path = self._make_output_path(index)
        self.csv_file = open(self.output_path, "w", newline="", encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.csv_file, fieldnames=COLUMNS, extrasaction="ignore")
        self.writer.writeheader()
        self.csv_file.flush()

    def open_spider(self, spider):
        self.success = 0
        self.failed = 0
        self.total = 0

        if hasattr(spider, "input_file") and spider.input_file:
            try:
                df = pd.read_excel(spider.input_file)
                self.total = len(df)
            except:
                pass

        self.pbar = tqdm(total=self.total, desc="Scraping", unit="product",
                         bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

        input_file = getattr(spider, "input_file", None)
        self._base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
        self._output_dir = "transformed-done-new"
        os.makedirs(self._output_dir, exist_ok=True)

        self._file_index = 1
        self._open_csv(self._file_index)

    def write_row(self, row):
        full_row = {col: row.get(col, "") for col in COLUMNS}
        self.writer.writerow(full_row)
        self.csv_file.flush()
        # Rotate if 20 MB reached
        if os.path.getsize(self.output_path) >= MAX_FILE_BYTES:
            self.csv_file.close()
            self._file_index += 1
            self._open_csv(self._file_index)

    def get_resources_block(self, item):
        resources = item.get("resources", [])
        rows = []
        for idx, res in enumerate(resources, start=1):
            href = res.get("href", "")
            title = res.get("title", "Resource")
            row = f'''<li class="description-product-spec-link-pdf__row" index="{idx}">
                    <div class="description-product-spec-link-pdf__first-column">
                        <div class="description-product-spec-link-pdf__icon-contain">
                        <a class="description-product-spec-link-pdf__icon-link" href="{href}" rel="noopener noreferrer" target="_blank">
                            <span class="description-product-spec-link-pdf__icon" data-type="application/pdf"></span>
                            <span class="sr-only">opens in a new tab</span>
                        </a>
                        </div>
                        <div class="description-product-spec-link-pdf__title-contain">
                        <h2 class="description-product-spec-link-pdf__title">
                            <a href="{href}" rel="noopener noreferrer" target="_blank">{title}<span class="sr-only">opens in a new tab</span></a>
                        </h2>
                        </div>
                    </div>
                    <div class="description-product-spec-link-pdf__second-column">
                        <div class="description-product-spec-link-pdf__contain">
                        <a class="description-product-spec-link-pdf" href="{href}" target="_blank" title="Click here to download {title}">
                            View<span class="sr-only">opens in a new tab</span>
                        </a>
                        </div>
                    </div>
                    </li>'''.strip()
            rows.append(row)

        if not rows:
            return ""

        return (
            '<div class="cmp-container description-resource" id="description-resource">'
            "<h6>Resources</h6>"
            '<ul class="description-product-spec-link-pdf__row-contain">'
            + "".join(rows)
            + "</ul></div>"
        )

    def process_item(self, item, spider):
        images = item.get("images", [])
        variants = item.get("variants", [])
        custom_fields = item.get("custom_fields", [])

        for idx, res in enumerate(item.get("resources", []), start=1):
            href = res.get("href", "")
            title = res.get("title", "Resource")
            value = f"<a href='{href}' target='_blank'>{title}</a>"
            if len(value) <= 255:
                custom_fields.append({"name": f"Resource {idx}", "value": value})

        has_variants = len(variants) > 1
        custom_fields_str = json.dumps(custom_fields) if custom_fields else ""

        is_failed = not item.get("title")
        status = "FAIL" if is_failed else "SUCCESS"
        if is_failed:
            self.failed += 1
        else:
            self.success += 1

        self.pbar.update(1)
        self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")

        self.write_row({
            "Status": status,
            "URL": item.get("url", ""),
            "Item": "Product",
            "Name": item.get("title"),
            "Type": "physical",
            "SKU": "" if has_variants else item.get("sku"),
            "Inventory Tracking": "none",
            "Current Stock": 0,
            "Low Stock": 0,
            "Price": "" if has_variants else item.get("price"),
            "Cost Price": 0,
            "Retail Price": 0,
            "Sale Price": 0,
            "Brand ID": 50,  # Hoffman brand ID in BigCommerce
            "Channels": 1,
            "Categories Names": item.get("category"),
            "Categories": "",
            "Description": f'<div>{item.get("description", "")}{self.get_resources_block(item)}</div>',
            "Custom Fields": custom_fields_str,
            "Availability": item.get("availability"),
            "Page Title": item.get("page_title", ""),
            "Product URL": item.get("product_url", ""),
            "Meta Description": item.get("meta_description", ""),
            "Search Keywords": item.get("search_keywords", ""),
            "Meta Keywords": item.get("meta_keywords", ""),
            "UPC/EAN": item.get("upc"),
            "Manufacturer Part Number": item.get("sku"),
            "Free Shipping": False,
            "Fixed Shipping Cost": 0,
            "Weight": 0, "Width": 0, "Height": 0, "Depth": 0,
            "Is Visible": True,
            "Is Featured": False,
            "Warranty": "",
            "Tax Class": 0,
            "Product Condition": "New",
            "Show Product Condition": False,
            "Sort Order": 0,
        })

        for idx, img in enumerate(images, start=1):
            self.write_row({
                "Status": "",
                "Item": "Image",
                "Image URL (Import)": img,
                "Image Description": f"{item.get("title")} | Genesee Supply Co.",
                "Image is Thumbnail": idx == 1,
                "Image Sort Order": idx,
            })

        for variant in variants:
            self.write_row({
                "Status": "",
                "Item": "SKU",
                "SKU": variant.get("sku"),
                "Price": variant.get("price"),
            })

        return item

    def close_spider(self, spider):
        self.pbar.close()
        self.csv_file.close()
        print(f"\n✓ {self.success} success | ✗ {self.failed} failed | saved to {self._output_dir}/")


class ResourceDownloadPipeline:

    def open_spider(self, spider):
        input_file = getattr(spider, "input_file", None)
        base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
        self.download_dir = os.path.join("transformed-done-new", f"{base_name}-downloads")
        os.makedirs(self.download_dir, exist_ok=True)

    def process_item(self, item, spider):
        resources = item.get("resources", [])
        if not resources:
            return item

        updated_resources = []
        for res in resources:
            href = res.get("href", "")
            if not href:
                continue

            filename = os.path.basename(urlparse(href).path)
            local_path = os.path.join(self.download_dir, filename)

            try:
                import urllib.request
                urllib.request.urlretrieve(href, local_path)
                cdn_url = f"/content/hoffman/{filename}"
            except Exception as e:
                spider.logger.error(f"Failed to download {href}: {e}")
                cdn_url = href

            updated_resources.append({**res, "href": cdn_url, "filename": filename})

        item["resources"] = updated_resources
        return item


# # import pandas as pd
# # import os
# # import json
# # from tqdm import tqdm

# # class BigcommercePipeline:

# #     def open_spider(self, spider):
# #         self.rows = []
# #         self.total = 0
# #         self.success = 0
# #         self.failed = 0

# #         # Count total URLs for progress bar
# #         if hasattr(spider, "input_file") and spider.input_file:
# #             try:
# #                 df = pd.read_excel(spider.input_file)
# #                 self.total = len(df)
# #             except:
# #                 self.total = 0

# #         self.pbar = tqdm(total=self.total, desc="Scraping products", unit="product",
# #                          bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] ✓{postfix}")

# #     def process_item(self, item, spider):
# #         images = item.get("images", [])
# #         variants = item.get("variants", [])
# #         custom_fields = item.get("custom_fields", [])

# #         has_variants = len(variants) > 1
# #         custom_fields_str = json.dumps(custom_fields) if custom_fields else ""

# #         # Determine status
# #         is_failed = not item.get("title")
# #         status = "FAIL" if is_failed else "SUCCESS"
# #         if is_failed:
# #             self.failed += 1
# #         else:
# #             self.success += 1

# #         self.pbar.update(1)
# #         self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")

# #         # -----------------------
# #         # 1. MAIN PRODUCT ROW
# #         # -----------------------
# #         self.rows.append({
# #             "Status": status,
# #             "URL": item.get("url"),
# #             "Item": "Product",
# #             "Name": item.get("title"),
# #             "Type": "physical",
# #             "SKU": "" if has_variants else item.get("sku"),
# #             "Inventory Tracking": "none",
# #             "Current Stock": 0,
# #             "Low Stock": 0,
# #             "Price": "" if has_variants else item.get("price"),
# #             "Cost Price": 0,
# #             "Retail Price": 0,
# #             "Sale Price": 0,
# #             "Brand": item.get("brand"),
# #             "Channels": 0,
# #             "Categories": item.get("category"),
# #             "Description": item.get("description"),
# #             "Custom Fields": custom_fields_str,
# #             "Availability": item.get("availability"),
# #             "Page Title": "",
# #             "Product URL": "",
# #             "Meta Description": "",
# #             "Search Keywords": "",
# #             "Meta Keywords": "",
# #             "Bin Picking Number": "",
# #             "UPC/EAN": item.get("upc"),
# #             "Global Trade Number": "",
# #             "Manufacturer Part Number": item.get("sku"),
# #             "Free Shipping": False,
# #             "Fixed Shipping Cost": 0,
# #             "Weight": 0,
# #             "Width": 0,
# #             "Height": 0,
# #             "Depth": 0,
# #             "Is Visible": True,
# #             "Is Featured": False,
# #             "Warranty": "",
# #             "Tax Class": 0,
# #             "Product Condition": "New",
# #             "Show Product Condition": False,
# #             "Sort Order": 0,
# #         })

# #         # -----------------------
# #         # 2. IMAGE ROWS
# #         # -----------------------
# #         for idx, img in enumerate(images, start=1):
# #             self.rows.append({
# #                 "Status": "",
# #                 "Item": "Image",
# #                 "Name": "",
# #                 "Image URL (Import)": img,
# #                 "Image Description": "",
# #                 "Image is Thumbnail": idx == 1,
# #                 "Image Sort Order": idx,
# #             })

# #         # -----------------------
# #         # 3. VARIANT ROWS
# #         # -----------------------
# #         for variant in variants:
# #             self.rows.append({
# #                 "Status": "",
# #                 "Item": "SKU",
# #                 "Name": "",
# #                 "SKU": variant.get("sku"),
# #                 "Price": variant.get("price"),
# #                 "Option1 Value": variant.get("option1"),
# #             })

# #         return item

# #     def handle_error(self, failure, spider):
# #         """Call this from spider errback to track failed URLs"""
# #         self.failed += 1
# #         self.pbar.update(1)
# #         self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")
# #         self.rows.append({
# #             "Status": "FAIL",
# #             "URL": failure.request.url,
# #             "Item": "",
# #             "Name": "",
# #         })

# #     def close_spider(self, spider):
# #         self.pbar.close()
# #         print(f"\n✓ Success: {self.success} | ✗ Failed: {self.failed} | Total: {self.total}")

# #         df = pd.DataFrame(self.rows)

# #         columns = [
# #             "Status",
# #             "URL",
# #             "Item",
# #             "Name",
# #             "Type",
# #             "SKU",
# #             "Options",
# #             "Inventory Tracking",
# #             "Current Stock",
# #             "Low Stock",
# #             "Price",
# #             "Cost Price",
# #             "Retail Price",
# #             "Sale Price",
# #             "Brand",
# #             "Channels",
# #             "Categories",
# #             "Description",
# #             "Custom Fields",
# #             "Availability",
# #             "Page Title",
# #             "Product URL",
# #             "Meta Description",
# #             "Search Keywords",
# #             "Meta Keywords",
# #             "Bin Picking Number",
# #             "UPC/EAN",
# #             "Global Trade Number",
# #             "Manufacturer Part Number",
# #             "Free Shipping",
# #             "Fixed Shipping Cost",
# #             "Weight",
# #             "Width",
# #             "Height",
# #             "Depth",
# #             "Is Visible",
# #             "Is Featured",
# #             "Warranty",
# #             "Tax Class",
# #             "Product Condition",
# #             "Show Product Condition",
# #             "Sort Order",
# #             "Variant Image URL",
# #             "Internal Image URL (Export)",
# #             "Image URL (Import)",
# #             "Image Description",
# #             "Image is Thumbnail",
# #             "Image Sort Order",
# #             "YouTube ID",
# #             "Video Title",
# #             "Video Description",
# #             "Video Sort Order",
# #         ]

# #         for col in columns:
# #             if col not in df.columns:
# #                 df[col] = ""

# #         df = df[columns]

# #         input_file = getattr(spider, "input_file", None)
# #         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"

# #         output_dir = "transformed_done"
# #         os.makedirs(output_dir, exist_ok=True)
# #         output_path = os.path.join(output_dir, f"{base_name}-import.csv")

# #         df.to_csv(output_path, index=False, encoding="utf-8-sig")
# #         print(f"Saved to: {output_path}")


# import pandas as pd
# import os
# import json
# import csv
# from tqdm import tqdm
# import logging
# from urllib.parse import urlparse

# logging.getLogger("scrapy").setLevel(logging.ERROR)
# logging.getLogger("scrapy-playwright").setLevel(logging.ERROR)
# logging.getLogger("playwright").setLevel(logging.ERROR)

# COLUMNS = [
#     "Status", "URL", "Item", "Name", "Type", "SKU", "Options",
#     "Inventory Tracking", "Current Stock", "Low Stock", "Price",
#     "Cost Price", "Retail Price", "Sale Price", "Brand", "Channels",
#     "Categories", "Description", "Custom Fields", "Availability",
#     "Page Title", "Product URL", "Meta Description", "Search Keywords",
#     "Meta Keywords", "Bin Picking Number", "UPC/EAN", "Global Trade Number",
#     "Manufacturer Part Number", "Free Shipping", "Fixed Shipping Cost",
#     "Weight", "Width", "Height", "Depth", "Is Visible", "Is Featured",
#     "Warranty", "Tax Class", "Product Condition", "Show Product Condition",
#     "Sort Order", "Variant Image URL", "Internal Image URL (Export)",
#     "Image URL (Import)", "Image Description", "Image is Thumbnail",
#     "Image Sort Order", "YouTube ID", "Video Title", "Video Description",
#     "Video Sort Order",
# ]

# MAX_FILE_BYTES = 20 * 1024 * 1024   # 20 MB


# class BigcommercePipeline:

#     # ADD _make_output_path and _rotate helpers inside BigcommercePipeline class
#     # (place right after open_spider):

#     def _make_output_path(self, index: int) -> str:
#         suffix = f"-part{index}" if index > 1 else ""
#         return os.path.join(self._output_dir, f"{self._base_name}{suffix}-import.csv")

#     def _open_csv(self, index: int):
#         self.output_path = self._make_output_path(index)
#         # self.csv_file = open(self.output_path, "w", newline="", encoding="utf-8-sig")
#         # self.writer = csv.DictWriter(self.csv_file, fieldnames=COLUMNS, extrasaction="ignore")
#         # self.writer.writeheader()
#         # self.csv_file.flush()
#         self._file_index = 1
#         self._output_dir = output_dir
#         self._base_name = base_name
#         self._open_csv(self._file_index)

#     def open_spider(self, spider):
#         self.success = 0
#         self.failed = 0

#         # Count total for progress bar
#         self.total = 0
#         if hasattr(spider, "input_file") and spider.input_file:
#             try:
#                 df = pd.read_excel(spider.input_file)
#                 self.total = len(df)
#             except:
#                 pass

#         self.pbar = tqdm(total=self.total, desc="Scraping", unit="product",
#                          bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

#         # Open CSV immediately and write header
#         input_file = getattr(spider, "input_file", None)
#         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
#         output_dir = "transformed-done"
#         os.makedirs(output_dir, exist_ok=True)
#         self.output_path = os.path.join(output_dir, f"{base_name}-import.csv")

#         self.csv_file = open(self.output_path, "w", newline="", encoding="utf-8-sig")
#         self.writer = csv.DictWriter(self.csv_file, fieldnames=COLUMNS, extrasaction="ignore")
#         self.writer.writeheader()
#         self.csv_file.flush()

#     def write_row(self, row):
#         """Write a single row and flush immediately to disk."""
#         # Fill missing columns with empty string
#         full_row = {col: row.get(col, "") for col in COLUMNS}
#         self.writer.writerow(full_row)
#         self.csv_file.flush()  # ← key: writes to disk immediately, not buffer

#     def get_resources_block(self, item):
#         resources = item.get("resources", [])
#         rows = []
#         for idx, res in enumerate(resources, start=1):
#             href = res.get("href", "")
#             title = res.get("title", "Resource")
#             row = f'''<li class="description-product-spec-link-pdf__row" index="{idx}">
#                     <div class="description-product-spec-link-pdf__first-column">
#                         <div class="description-product-spec-link-pdf__icon-contain">
#                         <a class="description-product-spec-link-pdf__icon-link" href="{href}" rel="noopener noreferrer" target="_blank">
#                             <span class="description-product-spec-link-pdf__icon" data-type="application/pdf"></span>
#                             <span class="sr-only">opens in a new tab</span>
#                         </a>
#                         </div>
#                         <div class="description-product-spec-link-pdf__title-contain">
#                         <h2 class="description-product-spec-link-pdf__title">
#                             <a href="{href}" rel="noopener noreferrer" target="_blank">{title}<span class="sr-only">opens in a new tab</span></a>
#                         </h2>
#                         </div>
#                     </div>
#                     <div class="description-product-spec-link-pdf__second-column">
#                         <div class="description-product-spec-link-pdf__contain">
#                         <a class="description-product-spec-link-pdf" href="{href}" target="_blank" title="Click here to download {title}">
#                             View<span class="sr-only">opens in a new tab</span>
#                         </a>
#                         </div>
#                     </div>
#                     </li>'''.strip()
#             rows.append(row)

#         resource_html = ""
#         if rows:
#             resource_html = (
#                 '<div class="cmp-container description-resource" id="description-resource">'
#                 "<h6>Resources</h6>"
#                 '<ul class="description-product-spec-link-pdf__row-contain">'
#                 + "".join(rows)
#                 + "</ul></div>"
#             )

#         return resource_html

#     def process_item(self, item, spider):
#         images = item.get("images", [])
#         variants = item.get("variants", [])
#         custom_fields = item.get("custom_fields", [])
#         for idx, res in enumerate(item.get("resources", []), start=1):
#             href = res.get("href", "")
#             title = res.get("title", "Resource")
#             value = f'<a href="{href}" target="_blank">{title}</a>'
#             if len(value) <= 255:
#                 custom_fields.append({"name": f"Resource {idx}", "value": value})

#         has_variants = len(variants) > 1
#         custom_fields_str = json.dumps(custom_fields) if custom_fields else ""

#         is_failed = not item.get("title")
#         status = "FAIL" if is_failed else "SUCCESS"
#         if is_failed:
#             self.failed += 1
#         else:
#             self.success += 1

#         self.pbar.update(1)
#         self.pbar.set_postfix_str(f"✓{self.success} ✗{self.failed}")

#         # Product row
#         self.write_row({
#             "Status": status,
#             "URL": item.get("url", ""),
#             "Item": "Product",
#             "Name": item.get("title"),
#             "Type": "physical",
#             "SKU": "" if has_variants else item.get("sku"),
#             "Inventory Tracking": "none",
#             "Current Stock": 0,
#             "Low Stock": 0,
#             "Price": "" if has_variants else item.get("price"),
#             "Cost Price": 0,
#             "Retail Price": 0,
#             "Sale Price": 0,
#             "Brand": item.get("brand"),
#             "Channels": 1,
#             "Categories": item.get("category"),
#             "Description": f'{item.get("description")}{self.get_resources_block(item)}',
#             "Custom Fields": custom_fields_str,
#             "Availability": item.get("availability"),
#             "UPC/EAN": item.get("upc"),
#             "Manufacturer Part Number": item.get("sku"),
#             "Free Shipping": False,
#             "Fixed Shipping Cost": 0,
#             "Weight": 0, "Width": 0, "Height": 0, "Depth": 0,
#             "Is Visible": True,
#             "Is Featured": False,
#             "Warranty": "",
#             "Tax Class": 0,
#             "Product Condition": "New",
#             "Show Product Condition": False,
#             "Sort Order": 0,
#         })

#         # Image rows
#         for idx, img in enumerate(images, start=1):
#             self.write_row({
#                 "Status": "",
#                 "Item": "Image",
#                 "Image URL (Import)": img,
#                 "Image is Thumbnail": idx == 1,
#                 "Image Sort Order": idx,
#             })

#         # Variant rows
#         for variant in variants:
#             self.write_row({
#                 "Status": "",
#                 "Item": "SKU",
#                 "SKU": variant.get("sku"),
#                 "Price": variant.get("price"),
#             })

#         return item

#     def close_spider(self, spider):
#         self.pbar.close()
#         self.csv_file.close()
#         print(f"\n✓ {self.success} success | ✗ {self.failed} failed | saved to {self.output_path}")



# class ResourceDownloadPipeline:

#     def open_spider(self, spider):
#         input_file = getattr(spider, "input_file", None)
#         base_name = os.path.splitext(os.path.basename(input_file))[0] if input_file else "output"
#         self.download_dir = os.path.join("transformed-done", f"{base_name}-downloads")
#         os.makedirs(self.download_dir, exist_ok=True)

#     def process_item(self, item, spider):
#         resources = item.get("resources", [])  # list of {"href": ..., "title": ...}
#         if not resources:
#             return item

#         updated_resources = []
#         for res in resources:
#             href = res.get("href", "")
#             if not href:
#                 continue

#             filename = os.path.basename(urlparse(href).path)
#             local_path = os.path.join(self.download_dir, filename)

#             # Download synchronously (Scrapy pipeline process_item is sync)
#             try:
#                 import urllib.request
#                 urllib.request.urlretrieve(href, local_path)
#                 cdn_url = f"/content/hoffman/{filename}"
#             except Exception as e:
#                 spider.logger.error(f"Failed to download {href}: {e}")
#                 cdn_url = href  # fallback to original

#             updated_resources.append({**res, "href": cdn_url, "filename": filename})

#         item["resources"] = updated_resources
#         return item

