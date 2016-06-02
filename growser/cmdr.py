from collections import Iterable
from inspect import getmembers
from types import FunctionType, GeneratorType, ModuleType
from typing import Callable, Generic, TypeVar


class Message:
    """Base class for both commands and domain events."""


class Command(Message):
    """Objects that mutate state."""


class DomainEvent(Message):
    """Objects that result from state mutation."""


class Query(Message):
    """Objects for requesting data."""


#: Generic :class:`Command` type
T = TypeVar('T')


class Handles(Generic[T]):
    """Indicates that a class handles instances of command type `T`.

    Example::

        class SingleCommandHandler(Handles[SingleCommand]):
            def handle(cmd: SingleCommand):
                pass

    Classes can also handle multiple commands::

        class MultiHandler(Handles[FirstCommand], Handles[SecondCommand]):
            def handle_first(cmd: FirstCommand):
                pass

            def handle_second(cmd: SecondCommand:)
                pass
    """


def handles_decorator():
    handlers = {}

    def wrapper(command):
        def inner(func):
            handlers[func] = command
            return func
        return inner
    wrapper.handlers = handlers
    return wrapper

#: Decorator for registering command handlers
handles = handles_decorator()


class Handler:
    __slots__ = ['klass', 'func']

    def __init__(self, klass: type, func: Callable):
        """A single callable used as a handler.

        This makes it easier to create instances of :attr:`klass` when the
        handler is a class method.

        :param klass: Class instance `func` is expecting.
        :param func: Callable responsible for handling commands of type `klass.
        """
        self.klass = klass
        self.func = func

    def __call__(self):
        func = self.func
        if self.klass:
            func = getattr(self.klass(), self.func.__name__)
        return func

    def __repr__(self):
        return "Handler<{} {}>".format(self.klass, self.func.__qualname__)


class HandlerInvoker:
    __slots__ = ['klass', 'handler']

    def __init__(self, klass: type, handler: Callable[..., FunctionType]):
        """Intermediary between the executing command bus and handler.

        :param klass: Type of class that is being executed
        :param handler: Callable that executes instance of this type
        """
        self.klass = klass
        self.handler = handler

    def execute(self, command: Command):
        """Execute the command using the registered command handler.

        :param command: An instance of :attr:`klass`.
        """
        results = self.handler(command)
        if isinstance(results, GeneratorType):
            results = list(results)
        return results

    def __repr__(self):
        return '<{} {} {}>'.format(
            self.__class__.__name__, self.klass.__name__, self.handler)


class Registry:
    """Scans modules & classes for handlers used for `commands` and `queries`.

    Example::

        registry = HandlerRegistry()
        registry.register(growser.handlers.events)
    """
    def __init__(self):
        self.handlers = {}

    def find(self, klass: type) -> Handler:
        """Return the handler assigned to `klass`."""
        return self.handlers.get(klass, None)

    def scan(self, obj):
        """Register a module, class, or function as a handler.

        :param obj: Object to register as a handler.
        """
        handlers = []
        if isinstance(obj, ModuleType):
            handlers = scan_module(obj)
        elif isinstance(obj, type):
            handlers = scan_class(obj)
        elif isinstance(obj, FunctionType):
            handlers = scan_function(obj)

        for klass, handler in handlers:
            if issubclass(klass, Command) and klass in self.handlers:
                raise DuplicateHandlerError(klass, handler)
            self.handlers[klass] = handler

        if not len(handlers):
            raise TypeError('Invalid command handler')

    def __iter__(self):
        yield from self.handlers.items()


def scan_module(module: ModuleType):
    """Scan a module for handlers."""
    if type(module) != ModuleType:
        raise TypeError('Module required')

    rv = []
    for obj in getmembers(module):
        if isinstance(obj[1], type) and issubclass(obj[1], Handles):
            rv += scan_class(obj[1])
        if type(obj[1]) == FunctionType:
            rv += scan_function(obj[1])
    return rv


def scan_class(klass: type):
    """Scan a class for handlers."""
    if not isinstance(klass, type):
        raise TypeError('Class required')

    # Commands via : Handles[T]
    expected = []
    if hasattr(klass, '__parameters__'):
        expected = [k for k in klass.__parameters__
                    if issubclass(k, Command)]

    def is_func(f):
        return type(f) == FunctionType and f.__name__[0] != '_'

    rv = []
    for func in [obj[1] for obj in getmembers(klass) if is_func(obj[1])]:
        rv += scan_function(func, klass)

    # Handlers declared by the class but not found
    commands = [cmd[0] for cmd in rv]
    missing = [cmd for cmd in expected if cmd not in commands]
    if len(missing):
        raise UnboundCommandError(missing)

    return rv


def scan_function(obj, klass: type=None):
    """Determine if a function or unbound class method is a handler.

    The class bound to the function is determined by either the presence
    of a type hint::

        def handles(cmd: Klass):

    Or a decorator::

        @handles(Klass)
        def handles(cmd):

    :param obj: Function to register as a handler.
    :return
    """
    if type(obj) != FunctionType:
        raise TypeError('Expected FunctionType')

    rv = []
    # Method type hints e.g. def name(command: Type)
    for param, param_type in obj.__annotations__.items():
        if issubclass(param_type, Message) and param != 'return':
            rv.append((param_type, Handler(klass, obj)))

    # Decorators using @handles(CommandType)
    if hasattr(handles, 'handlers') and obj in handles.handlers:
        rv.append((handles.handlers[obj], Handler(klass, obj)))

    return rv


class LocalCommandBus:
    """Experimental command bus for executing messages in the local context."""
    def __init__(self, registry: Registry):
        self.registry = registry

    def execute(self, cmd: Command) -> None:
        """Execute a command"""
        handler = self.registry.find(cmd.__class__)
        if not handler:
            raise LookupError('No handler found for {}'.format(cmd.__class__))

        invoker = HandlerInvoker(handler.klass, handler())
        rv = invoker.execute(cmd)

        if isinstance(rv, Iterable):
            for result in rv:
                if isinstance(result, Command):
                    self.execute(result)
                if isinstance(result, DomainEvent):
                    self.publish(result)

        return rv

    def publish(self, event: DomainEvent):
        pass


class DuplicateHandlerError(Exception):
    """Command is bound to multiple handlers."""
    def __init__(self, command: type, duplicate):
        super().__init__('Duplicate handler found for {}: {}'.format(
            command.__name__, duplicate))


class UnboundCommandError(Exception):
    """Handle[T] present but handler for `T` not found."""
    def __init__(self, commands):
        super().__init__('Commands bound without handlers: {}'.format(
            ', '.join(map(lambda x: x.__name__, commands))))
