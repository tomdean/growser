from typing import Iterable
import unittest

from growser.cmdr import (
    Command,
    Registry,
    DomainEvent,
    DuplicateHandlerError,
    handles,
    Handles,
    Handler,
    HandlerInvoker,
    LocalCommandBus,
    UnboundCommandError,
    scan_class,
    scan_module,
    scan_function
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


def init_registry(handler):
    registry = Registry()
    registry.scan(handler)
    return registry


class RegistryTestCase(unittest.TestCase):
    def test_can_add_func_with_type_hint(self):
        def handler(cmd: FakeCommand):
            pass
        manager = init_registry(handler)
        assert manager.find(FakeCommand).func == handler

    def test_can_add_func_with_decorator(self):
        @handles(FakeCommand)
        def handler(cmd):
            pass
        manager = init_registry(handler)
        assert manager.find(FakeCommand).func == handler

    def test_invalid_function_fails(self):
        def handler(cmd):
            pass
        manager = Registry()
        with self.assertRaises(TypeError):
            manager.scan(handler)

    def test_can_add_class_method_with_type_hint(self):
        manager = init_registry(FakeCommandHandler.handle)
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle

    def test_cannot_add_invalid_callable(self):
        manager = Registry()
        with self.assertRaises(TypeError):
            manager.scan("not a valid callable")

    def test_cannot_add_class_with_multiple_handlers(self):
        class FakeCommandHandlerMultipleHandlers(Handles[FakeCommand]):
            def handle(self, cmd: FakeCommand):
                pass

            def handle2(self, cmd: FakeCommand):
                pass

        with self.assertRaises(DuplicateHandlerError):
            init_registry(FakeCommandHandlerMultipleHandlers)

    def test_cannot_add_class_with_non_class(self):
        with self.assertRaises(TypeError):
            init_registry(lambda x: x)

    def test_can_add_class_with_decorator(self):
        class FakeCommandHandler(Handles[FakeCommand]):
            @handles(FakeCommand)
            def handle(self, cmd):
                pass

        manager = init_registry(FakeCommandHandler)
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle

    def test_can_add_class_with_type_hint(self):
        class FakeCommandHandler(Handles[FakeCommand]):
            def handle(self, cmd: FakeCommand):
                pass

        manager = init_registry(FakeCommandHandler)
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle

    def test_cannot_add_class_bound_without_handler(self):
        class FakeCommandHandler(Handles[FakeCommand]):
            pass

        with self.assertRaises(UnboundCommandError):
            init_registry(FakeCommandHandler)

    def test_can_add_class_with_multiple_handlers(self):
        class FakeCommandHandler(Handles[FakeCommand], Handles[FakeCommand2]):
            def handle1(self, cmd: FakeCommand):
                pass

            def handle2(self, cmd: FakeCommand2):
                pass

        manager = init_registry(FakeCommandHandler)
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle1
        assert manager.find(FakeCommand2).func == FakeCommandHandler.handle2

    def test_can_add_module(self):
        import sys

        manager = init_registry(sys.modules[__name__])
        assert manager.find(FakeCommand).func == FakeCommandHandler.handle
        assert manager.find(FakeCommand2).func == FakeCommand2Handler.handle

    def test_failures(self):
        with self.assertRaises(TypeError):
            scan_class(lambda x: x)
        with self.assertRaises(TypeError):
            scan_module(lambda x: x)
        with self.assertRaises(TypeError):
            scan_function(22)

    def test_iter(self):
        manager = Registry()
        manager.scan(FakeCommandHandler)
        assert len(list(manager)) == 1
        manager.scan(FakeCommand2Handler)
        assert len(list(manager)) == 2


class HandlerInvokerTests(unittest.TestCase):
    def test_can_invoke_function(self):
        called = []
        def handler(cmd):
            called.append(cmd)

        handler = HandlerInvoker(FakeCommand, handler)
        cmd = FakeCommand()
        handler.execute(cmd)

        assert len(called) > 0

    def test_generators(self):
        def handler(cmd):
            yield 1
            yield 2
            yield 3

        invoker = HandlerInvoker(FakeCommand, handler)
        rv = invoker.execute(FakeCommand())
        assert rv == [1, 2, 3]


class CallableTests(unittest.TestCase):
    def test_can_call_function(self):
        called = []
        def handler(cmd):
            called.append(True)
        func = Handler(None, handler)
        assert func() == handler

    def test_can_call_method(self):
        called = []
        class FakeHandler(Handles[FakeCommand]):
            def handler(cmd):
                called.append(True)
        func = Handler(FakeHandler, FakeHandler.handler)
        assert isinstance(func().__self__, FakeHandler)


class LocalCommandBusTests(unittest.TestCase):
    def test_execute(self):
        manager = Registry()
        manager.scan(FakeCommandHandler)
        bus = LocalCommandBus(manager)
        assert isinstance(bus.execute(FakeCommand()), FakeEvent)

    def test_fails_no_handler(self):
        manager = Registry()
        bus = LocalCommandBus(manager)
        with self.assertRaises(LookupError):
            bus.execute("test")
