from growser.cmdr import CommandHandlerManager, LocalCommandBus


def configure(app):
    from growser.handlers import media, rankings

    handlers = CommandHandlerManager()
    handlers.register(media)
    handlers.register(rankings)
    return LocalCommandBus(handlers)
