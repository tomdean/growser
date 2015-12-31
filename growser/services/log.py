import logging.config


def configure(app, cfg=None):
    app.debug = app.logger.name and app.config.get('DEBUG', False)
    if cfg:
        logging.config.fileConfig(cfg)
    return app.logger
