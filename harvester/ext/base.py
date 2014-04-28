class PluginBase(object):
    options = []  # (name, type, default, doc)

    def __init__(self, url, conf=None):
        self.url = url
        self.conf = conf or {}
