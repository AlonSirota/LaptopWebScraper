import typing
from urllib.parse import urlparse, parse_qs, urlunparse, ParseResult, urlencode
class Url:
    """
    Site's URL with helper methods
    """

    def __init__(self, url):
        self.parsed_url = urlparse(url)

    def get_url(self, path: str):
        """
        Returns the url with specific file path
        """
        return urlunparse(
            self.parsed_url._replace(path=path))
    
    def get_page_url(self, page_index: int):
        """
        Returns the url with specific catalog page
        """
        parsed_quary = parse_qs(self.parsed_url.query)
        parsed_quary["list"] = "1" # This query argument controls the catalog page
        modified_query = urlencode(parsed_quary, doseq=True)

        return urlunparse(self.parsed_url._replace(query=modified_query))

parsed_url: ParseResult