from c4.system.rest import BaseRequestHandler

class Console(BaseRequestHandler):
    """
    Basic web console handler
    """
    URL_PATTERN = r"/index.html"

    def get(self):
        self.render("index.html")
