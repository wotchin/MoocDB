from .basic import CatalogForm, CatalogBasic


class CatalogFunctionForm(CatalogForm):
    def __init__(self, function_name, arg_num, callback, is_agg=False):
        self.function_name = function_name
        self.arg_num = arg_num
        self.callback = callback
        self.agg_function = is_agg


class CatalogFunction(CatalogBasic):
    def __init__(self):
        super().__init__('function_information')

    def dump(self):
        pass

    def load(self):
        pass
