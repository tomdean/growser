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


class DuplicateHandlerError(Exception):
    """Command is bound to multiple handlers."""
    def __init__(self, command: type, duplicate):
        super().__init__('Duplicate handler found for {}: {}'.format(
            command.__name__, duplicate))


class UnboundCommandError(Exception):
    """Handle[T] inherited but handler not found."""
    def __init__(self, commands):
        super().__init__('Commands bound without handlers: {}'.format(
            ', '.join(map(lambda x: x.__name__, commands))))


#: Generic :class:`Command` type
T = TypeVar('T')


class Handles(Generic[T]):
    """Indicates that a class handles instances of command type `T`."""


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
    """Intermediary between a function/method and the executing context."""
    def __init__(self, klass: type, func: Callable):
        self.klass = klass
        self.func = func

    def __call__(self):
        func = self.func
        if self.klass:
            func = getattr(self.klass(), self.func.__name__)
        return func

    def __repr__(self):
        return "Handler<{} {}>".format(
            self.klass.__name__, self.func.__qualname__)


class HandlerInvoker:
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


class HandlerRegistry:
    def __init__(self):
        self.handlers = {}

    def find(self, klass: type) -> Handler:
        """Return the handler assigned to `klass`."""
        return self.handlers.get(klass, None)

    def register(self, obj):
        """Register a module, class, or function as a handler.

        :param obj: Object to register as a handler.
        """
        handlers = []
        if type(obj) == ModuleType:
            handlers = self._scan_module(obj)
        if type(obj) == FunctionType:
            handlers = self._scan_function(obj)
        if isinstance(obj, type):
            handlers = self._scan_class(obj)

        for klass, handler in handlers:
            if issubclass(klass, Command) and klass in self.handlers:
                raise DuplicateHandlerError(klass, handler)
            self.handlers[klass] = handler

        if not len(handlers):
            raise TypeError('Invalid command handler type')

    def _scan_module(self, module: ModuleType):
        """Scan a module for handlers."""
        if type(module) != ModuleType:
            raise TypeError('Module required')

        rv = []
        for obj in getmembers(module):
            if isinstance(obj[1], type) and issubclass(obj[1], Handles):
                rv += self._scan_class(obj[1])
            if type(obj[1]) == FunctionType:
                rv += self._scan_function(obj[1])
        return rv

    def _scan_class(self, klass: type):
        """Scan a class for handlers."""
        if not isinstance(klass, type):
            raise TypeError('Class type required')

        # Commands via : Handles[T]
        expected = []
        if hasattr(klass, '__parameters__'):
            expected = [k for k in klass.__parameters__
                        if issubclass(k, Command)]

        def is_func(f):
            return type(f) == FunctionType and f.__name__[0] != '_'

        funcs = []
        rv = []
        for func in [obj[1] for obj in getmembers(klass) if is_func(obj[1])]:
            funcs.append((func, klass))
            rv += self._scan_function(func, klass)

        # If a callable hasn't been found but the class only has a single
        # method with a single argument, use that as a fallback.

        # Handlers declared by the class but not found
        commands = [cmd[0] for cmd in rv]
        missing = [cmd for cmd in expected if cmd not in commands]
        if len(missing):
            raise UnboundCommandError(missing)

        return rv

    def _scan_function(self, obj, klass: type=None):
        """Register a function or unbound class method as a handler.

        The class bound to the function is determined by either the presence
        of a type hint::

            def handles(cmd: Klass):

        Or a decorator::

            @handles(Klass)
            def handles(cmd):

        :param obj: Function to register as a handler.
        """
        if type(obj) != FunctionType:
            raise TypeError('Expected FunctionType')

        rv = []
        # Method type hints e.g. def name(command: Type)
        for param, param_type in obj.__annotations__.items():
            if issubclass(param_type, Command) and param != 'return':
                rv.append((param_type, Handler(klass, obj)))

        # Decorators using @handles(CommandType)
        if hasattr(handles, 'handlers') and obj in handles.handlers:
            rv.append((handles.handlers[obj], Handler(klass, obj)))

        return rv

    def __iter__(self):
        yield from self.handlers.items()


class LocalCommandBus:
    def __init__(self, registry: HandlerRegistry):
        self.registry = registry

    def execute(self, cmd: Command):
        handler = self.registry.find(cmd.__class__)
        if not handler:
            raise LookupError('No handler found for {}'.format(cmd.__class__))
        invoker = HandlerInvoker(handler.klass, handler())
        return invoker.execute(cmd)
