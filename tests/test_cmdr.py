import unittest

from growser.cmdr import (
    Callable,
    Command,
    CommandHandlerManager,
    CommandHandlerInvoker,
    DomainEvent,
    DuplicateHandlerError,
    handles,
    Handles,
    LocalCommandBus,
    UnboundCommandError
)


class FakeEvent(DomainEvent):
    pass


class FakeCommand(Command):
    def __init__(self, name="test"):
        self.name = name


class FakeCommand2(Command):
    pass


class FakeCommandHandler(Handles[FakeCommand]):
    def handle(self, cmd: FakeCommand):
        return FakeEvent()


class FakeCommand2Handler(Handles[FakeCommand2]):
    def handle(self, cmd: FakeCommand2):
        pass


class CommandHandlerManagerTestCase(unittest.TestCase):
    def test_can_add_func_with_type_hint(self):
        def handler(cmd: FakeCommand):
            pass
        manager = CommandHandlerManager()
        manager.register(handler)
        assert manager.find(FakeCommand).func == handler

    def test_can_add_func_with_decorator(self):
        @handles(FakeCommand)
        def handler(cmd):
            pass
        manager = CommandHandlerManager()
        manager.register(handler)
        assert manager.find(FakeCommand).func == handler

    def test_invalid_function_fails(self):
        def handler(cmd):
            pass
        manager = CommandHandlerManager()
        manager.register(handler)
        assert not manager.find(FakeCommand)

    def test_can_add_class_method_with_type_hint(self):
        manager = CommandHandlerManager()
        manager.add_function(FakeCommandHandler.handle)

        assert manager.find(FakeCommand).func == FakeCommandHandler.handle

    def test_cannot_add_function_with_non_function(self):
        manager = CommandHandlerManager()
        with self.assertRaises(TypeError):
            manager.add_function(FakeCommand2Handler)

    def test_cannot_add_invalid_callable(self):
        manager = CommandHandlerManager()
        with self.assertRaises(TypeError):
            manager.register("not a valid callable")

    def test_cannot_add_class_with_multiple_handlers(self):
        class FakeCommandHandlerMultipleHandlers(Handles[FakeCommand]):
            def handle(self, cmd: FakeCommand):
                pass

            def handle2(self, cmd: FakeCommand):
                pass

        manager = CommandHandlerManager()
        with self.assertRaises(DuplicateHandlerError):
            manager.add_class(FakeCommandHandlerMultipleHandlers)

    def test_cannot_add_class_with_non_class(self):
        manager = CommandHandlerManager()
        with self.assertRaises(TypeError):
            manager.add_class(lambda x: x)

    def test_can_add_class_with_decorator(self):
        class FakeCommandHandler(Handles[FakeCommand]):
            @handles(FakeCommand)
            def handle(self, cmd):
                pass

        manager = CommandHandlerManager()
        manager.register(FakeCommandHandler)
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle

    def test_can_add_class_with_type_hint(self):
        class FakeCommandHandler(Handles[FakeCommand]):
            def handle(self, cmd: FakeCommand):
                pass

        manager = CommandHandlerManager()
        manager.register(FakeCommandHandler)
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle

    def test_cannot_add_class_bound_without_handler(self):
        class FakeCommandHandler(Handles[FakeCommand]):
            pass

        manager = CommandHandlerManager()
        with self.assertRaises(UnboundCommandError):
            manager.add_class(FakeCommandHandler)

    def test_can_add_class_with_multiple_handlers(self):
        class FakeCommandHandler(Handles[FakeCommand], Handles[FakeCommand2]):
            def handle1(self, cmd: FakeCommand):
                pass
            def handle2(self, cmd: FakeCommand2):
                pass

        manager = CommandHandlerManager()
        manager.register(FakeCommandHandler)

        assert manager.find(FakeCommand).func == FakeCommandHandler.handle1
        assert manager.find(FakeCommand2).func == FakeCommandHandler.handle2

    def test_can_add_module(self):
        import sys

        manager = CommandHandlerManager()
        manager.register(sys.modules[__name__])

        assert manager.find(FakeCommand).func == FakeCommandHandler.handle
        assert manager.find(FakeCommand2).func == FakeCommand2Handler.handle

    def test_cannot_add_class_as_module(self):
        manager = CommandHandlerManager()

        with self.assertRaises(TypeError):
            manager.add_module(FakeCommandHandler)


class CommandHandlerInvokerTests(unittest.TestCase):
    def test_can_invoke_function(self):
        called = []
        def handler(cmd):
            called.append(cmd)

        handler = CommandHandlerInvoker(FakeCommand, handler)
        cmd = FakeCommand()
        handler.execute(cmd)

        assert len(called) > 0

    def test_generators(self):
        def handler(cmd):
            yield 1
            yield 2
            yield 3

        invoker = CommandHandlerInvoker(FakeCommand, handler)
        rv = invoker.execute(FakeCommand())
        assert rv == [1, 2, 3]


class CallableTests(unittest.TestCase):
    def test_can_call_function(self):
        called = []
        def handler(cmd):
            called.append(True)
        func = Callable(None, handler)
        assert func.handler() == handler

    def test_can_call_method(self):
        called = []
        class FakeHandler(Handles[FakeCommand]):
            def handler(cmd):
                called.append(True)
        func = Callable(FakeHandler, FakeHandler.handler)
        assert isinstance(func.handler().__self__, FakeHandler)


class LocalCommandBusTests(unittest.TestCase):
    def test_execute(self):
        manager = CommandHandlerManager()
        manager.register(FakeCommandHandler)
        bus = LocalCommandBus(manager)
        assert isinstance(bus.execute(FakeCommand()), FakeEvent)
