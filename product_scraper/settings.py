# Scrapy settings for product_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import logging

# LOG_LEVEL = "ERROR"
# LOG_SHORT_NAMES = True  # shorter prefixes if any logs do appear
# LOG_LEVEL = 'DEBUG'

# logging.getLogger("scrapy").setLevel(logging.ERROR)
# logging.getLogger("scrapy-playwright").setLevel(logging.ERROR)
# logging.getLogger("playwright").setLevel(logging.ERROR)

BOT_NAME = "product_scraper"

SPIDER_MODULES = ["product_scraper.spiders"]
NEWSPIDER_MODULE = "product_scraper.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "product_scraper (+http://www.yourdomain.com)"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'


# Obey robots.txt rules
ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 8

# Autothrottle handles pacing — don't set DOWNLOAD_DELAY alongside it
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 15
AUTOTHROTTLE_TARGET_CONCURRENCY = 4   # target 4 in-flight at a time, ramps up/down automatically
AUTOTHROTTLE_DEBUG = False             # set True temporarily to see throttle decisions in logs

PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 45000
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 8

COOKIES_ENABLED = False
COOKIES_DEBUG = False  # helpful during dev to see cookies being sent


# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

DOWNLOAD_HANDLERS = {
        "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": False,
} 

DEFAULT_REQUEST_HEADERS = {
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
    "Cookie": 'ldId=8ec04ebb-e48e-4ea9-8644-20f709196fbc; _gcl_au=1.1.856018857.1775830596; _ga=GA1.1.611480472.1775830597; _fbp=fb.1.1775830598410.321003803706637201; FPID=FPID2.2.5UVuh6KBD6N%2BXJAzIwD1iTs5KtIioWTTy0TGDDvjcqc%3D.1775830597; FPAU=1.1.856018857.1775830596; _gtmeec=e30%3D; usi_id=2qp4rd_1775830601; usi_coupon_user=2qp4rd_1775830601; sgtm_abmfs_info=FPID2.2.5UVuh6KBD6N%252BXJAzIwD1iTs5KtIioWTTy0TGDDvjcqc%253D.1775830597%7Cbot-header-undefined%7C; __pr.11gr=qjwwGdxykh; isAuthenticated=false; builder.userAttributes={"isAuthenticated":false,"isCookieSet":false,"userState":""}; FPLC=D5HnyJVOytwWsBLSZYQFOJwnn8EZ%2FyD1vQBfPJj7B1OVONV4XBONFKH3kqiM3qMfzbmKPGjEfCznGVUOYDd3L2f4DCM0vubjhAjsRbfMFSRxmRYjpSU9hFHcz6HCpQ%3D%3D; bm_mi=131B26733D4DCD8DF5FFF7388755AE28~YAAQFK3OF+1i8mSdAQAAEcgCtB94hgqf8MVnlRbJdta6cnRTKFB4vhHgGUJZyUm6fq9ZByBrV/DWG8pcipJNPMpzAln/sBX/IZGyVGaGEOKjNJyQt3CRwMXCnKBJItnj2Ro0sFgo49vc+bRhytox79qgrS2T5439e84zXPc/W3C/rPFR2ULjt12RGhKQg2vLEnEizFb7mXJ223Fw0kcLcpaDashN4TeXzfwICBQKvrXPCshHg9XZ6Phb8xN/uTDPVPU81gNCknXaRgECrd43lGJS24gZ6d3I78SaIHhX+Q4Kfo/mjOe8JRNE9RmrTRcZKehQ265VLuE8hL0=~1; ak_bmsc=BEE2F0E2BA8BE055EF8991F941D63520~000000000000000000000000000000~YAAQFK3OFytj8mSdAQAA19ICtB/hXBL4m5vD+W8KbTnyTCQ1FPqtCVGVyiwNOvy5/mrNoI8F8UC8cMv6hTZP66GHBMp2ULc0FWh97jI+C4a9IPwrY8GGbOtpJzUM9H/N1rT48zVzOxLMQUALmsqnhT/GkzgvV7roc8GpCcWEmQYUBfROLjZTjyxMAKMc2a3HF7SPtslu9tT5pbiPFmdBAbzzNXauWMSxBgB9HEBtFbmzFZ+o7dLSp6I2+pt2IS0k0SRk6tLepv0bsbMLLyikTw42F+XjU4aQsV8U1OULAdd4yAhnEmK85rI01n2J9wgve7ot+4r+rGd49PEyI+HhbdWijEo6WddlQ2pTqLm+NfzbNW/GReTFomv9VuCpZEOIQCV+5sP1OOX/oW4b7aKsXl2z/SfQu3TikqZ6Nd4UDoWWeIlgYR9ZDn2LOehGTwD9Dsi0EW18VXrhdeYv/tODrrCL4vZE3bm01mvZOF2GaZRuThLZD1nE1d3x8cI4yQ==; bm_sz=D59C10A6CC119A0ADD9D9520EF563762~YAAQFK3OF1h/8mSdAQAAz/gGtB/27/DdtLsOIuAKrhiyHEflJf8/ZJTN0PeLeSQk+RrOE/D0UCuHBF0D4frFNnaRNXZUOdAdn6ETfQ+MHrop+vYZuevFsruryPEDEdF0Q55vi2ApFgz+6r0zf7glDnsin6JFwP+rD9IurxEMM3ENERGvuIMj7eZ+32qjttdqpBfz9UaFODYJtf9xJq6r3XYAER5r1QHIgaQ1G9z/brm3edAR5Av7+4qLrAYEE1AyKz0CaazfQHIDRT2DDKfzqmGVHviyZBr+vIH1sPrR7+dHoP22BCTIquQm5G9lE+VTJCAvgCQhF7wYTw64ajKwkXLA24nafW3W42CtY8fuBhw9NReJYp6vylaOT+ZGammj5vndq3af6EClW0vtJiXXW/DzpIAzIBMlo4P8hkoNg/7a2JaebRGWCDuk9G3yxbyDLXSQLw==~4604468~3551556; page_count=15; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Apr+22+2026+12%3A40%3A53+GMT%2B0530+(India+Standard+Time)&version=202308.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=e22d3ed6-b4a4-4b65-93e0-795be9926c36&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0002%3A1%2CC0001%3A1%2CC0003%3A1&AwaitingReconsent=false; _uetsid=a5f549103e1611f1bc3779ca0b198db9|14wkpna|2|g5f|0|2303; fs_uid=^#o-1HR607-na1^#5751bd4c-f053-4fae-a7be-8167ed8f6272:a019db87-077b-4f54-97bd-cd370ec935e1:1776840241720::6^#2616dada^#^#/1807366661; _abck=8899E5537571DCFCABAFB24974C2B8CD~0~YAAQFK3OF4eA8mSdAQAAyhwHtA/9diTimH+410Evn8/PfSGLzD7tI32QF7r+YQI+ThuY/TzfNoX4yc0FD83MO/w4laxAj0lQw3QBGDTKTI+FLtZIRtY866YFDLRtcoxCbM67DjiJMKglbTxgeIVJFY+Lc/mzxOcJy7tfRTdjC8mbRs5tnJaCsXZWUlVCBlpARUu/1wkpsC/ntRKhS/sAEIYIdptFNag1P30NUl08+UNtzcz8H/TH+YXaP+RWv08j0TxCr102uf0SznjCNDnJyg1SA5IkLp5DChELu6pZhAukvrVcXC0B8yLUhb0nvQUKym6p/BWDbFkRE7vrWTxVbjb2jYiIa5dpJivtooq+dFF+EeHAk5Z/CFK/UshNGieyjsPOlQbz3YGUyBgldF0uP7jWU/lIfxop+pCdup+BKwhIAU3TO3zWK8buQwqCpsOHsTyXRwhz8JDKzgueJ2KsF/Il+o5Me2YZZ9IhSsVtozcumFN1u+AWeGChCdXQ9VdDylVth6mhDIlxtWy9SXTkseXfNHV78jKI1uJALRmniUB7nCx59aERa2zu2kL55J62lEoq/gYKhYmADJUngl3/VUrp26NzlRdYsCQ+Kz8q+VzbIdknmuVfvu4eG12st2tzkS3CS8HOOea/Ll//fpl0GqwoOV2WhTesveCb~-1~-1~-1~AAQAAAAF%2f%2f%2f%2f%2fzIi5zZvgKprwtwGfGDFUxNHViCvlCRyTSqqvKys7sonKMxHlGz85T3OvJ64ZycRvLy7Vi1dTRqzGLyFcg0p5KEVv3%2fD2oKmXHqGv%2faNHJ8DLw5uJp51ypMEveJ9zVEJHimKlTw%3d~-1; fs_lua=1.1776841906637; _uetvid=e4c2982034e711f1b9cd236f96bc8455|19ymie1|1776842253190|17|1|bat.bing.com/p/insights/c/k; FPGSID=1.1776842261.1776842261.G-JM60PMS287.8j__ZmnhC2BJarv39--1rA; _ga_JM60PMS287=GS2.1.s1776840242^$o4^$g1^$t1776842262^$j57^$l0^$h691394361^$dV6ty6Swi9HEn0nnVOASzQoRQYlI-vCETng; bm_sv=742BF89C70CFBB198149A5DAB9182607~YAAQD63OF9XvuGKdAQAA4k0NtB+RhXSEKJtIhi0OlDXBRd1h99OapHdqZoJxK4AtHz5geInT5QJsBA9pH1mJ9bkerYb6xZqJh+V8/HYsNjCSKLLS2W773Nrz4bvi+c8XVgJOsI9VqagqFp3fD30+DtO33xgCaeSHWiknRnab1JaaNcrjlaZ/+mJ3bmYk0VxOouQ91NaWBgVxacd6kaqslEKG/AVQamSoBSJlZP1uF5GUl9MgizDTa95GTX7JKACQ~1; datadome=EcU5jldQcP8nKgfLE7wQ_FdXUG9YoqM9dpOEcTbroWGvDUU4c43pShwKxPSM_ukb39LYib_F6NFtSt6oMew4VAmIN1NP7F5x0N1HghV20Phih734PTRRL~fDmgfbQ28_'
}
# DOWNLOADER_MIDDLEWARES = {
#     'myproject.middlewares.DebugHeadersMiddleware': 100,
# }
# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "product_scraper.middlewares.ProductScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "product_scraper.middlewares.ProductScraperDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "product_scraper.pipelines.ResourceDownloadPipeline": 200,  # runs first
    "product_scraper.pipelines.BigcommercePipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# BOT_NAME = "product_scraper"

# SPIDER_MODULES = ["product_scraper.spiders"]
# NEWSPIDER_MODULE = "product_scraper.spiders"

# ADDONS = {}

# ROBOTSTXT_OBEY = False

# # Concurrency — tabs within one shared context
# CONCURRENT_REQUESTS = 8
# CONCURRENT_REQUESTS_PER_DOMAIN = 8  # matches above since single domain
# DOWNLOAD_DELAY = 0                   # let autothrottle handle pacing

# DOWNLOAD_HANDLERS = {
#     "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
#     "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
# }

# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# PLAYWRIGHT_BROWSER_TYPE = "chromium"

# PLAYWRIGHT_LAUNCH_OPTIONS = {
#     "headless": True,
# }

# # Single shared context = tabs, not new browsers per request
# PLAYWRIGHT_CONTEXTS = {
#     "default": {
#         "viewport": {"width": 1280, "height": 800},
#         "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
#     }
# }
# PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 8  # must match CONCURRENT_REQUESTS

# DEFAULT_REQUEST_HEADERS = {
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#     "Accept-Language": "en-US,en;q=0.9",
#     "Accept-Encoding": "gzip, deflate, br",
#     "Connection": "keep-alive",
#     "Upgrade-Insecure-Requests": "1",
# }

# ITEM_PIPELINES = {
#     "product_scraper.pipelines.BigcommercePipeline": 300,
# }

# AUTOTHROTTLE_ENABLED = True
# AUTOTHROTTLE_START_DELAY = 0.5
# AUTOTHROTTLE_MAX_DELAY = 10
# AUTOTHROTTLE_TARGET_CONCURRENCY = 6.0  # maintain ~6 parallel tabs

# FEED_EXPORT_ENCODING = "utf-8"