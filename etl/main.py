from strategy import OtoMotoETL, ContextManager

params = {
    'delay_scraping': False,
    'how_add': 'append'
}

etl = OtoMotoETL()
context = ContextManager(etl)
context.set_params(**params)
context.run()