import logging.config


def configure(app):
    app.debug = app.logger.name and app.config.get('DEBUG', False)
    logging.config.fileConfig("logging.cfg")
    return app.logger
