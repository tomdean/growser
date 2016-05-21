import importlib

from growser.cmdr import CommandHandlerManager, LocalCommandBus


def configure(app):
    handlers = CommandHandlerManager()
    for module in app.config.get('CMDR_HANDLERS'):
        handlers.register(importlib.import_module(module))
    return LocalCommandBus(handlers)
