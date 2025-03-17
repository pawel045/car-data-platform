from strategy import OtoMotoETL, ContextManager

params = {
    'brand': '',
    'model': '',
    'days_ago': -1,
    'delay_scraping': False,
    'how_add': 'append'
}

etl = OtoMotoETL()
context = ContextManager(etl)
context.set_params(**params)
context.run()