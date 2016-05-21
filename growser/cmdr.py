from inspect import getmembers
import sys
from types import FunctionType, GeneratorType, ModuleType
from typing import Generic, TypeVar


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


class Callable:
    """Wrapper for a class method handler."""
    def __init__(self, klass: type, func):
        self.klass = klass
        self.func = func

    def handler(self):
        func = self.func
        if self.klass:
            func = getattr(self.klass(), self.func.__name__)
        return func

    def __repr__(self):
        return "<Callable {}>".format(self.func.__qualname__)


class CommandHandlerInvoker:
    def __init__(self, command_type: type, handler):
        """Manages the execution of a command by a command handler."""
        self.command_type = command_type
        self.handler = handler

    def execute(self, command: Command):
        """Execute the command using the registered command handler.

        :param command: An instance of :attr:`command_type`.
        """
        results = self.handler(command)
        if isinstance(results, GeneratorType):
            results = list(results)
        return results

    def __repr__(self):
        return 'CommandHandlerInvoker({}, {})'.format(
            self.command_type.__name__, self.handler)


class CommandHandlerManager:
    def __init__(self):
        self.handlers = {}

    def find(self, klass: type) -> Callable:
        """Return the handler assigned to `command`."""
        return self.handlers.get(klass, None)

    def _add(self, command: type, handler: Callable):
        """Register a command and its associated handler.

        :param command: Command type
        :param handler: Handler callable
        """
        if issubclass(command, Command) and command in self.handlers:
            raise DuplicateHandlerError(command, handler)
        self.handlers[command] = handler

    def register(self, obj):
        """Register a module, class, or function as a command handler.

        :param obj: Object to register as a command handler.
        """
        if type(obj) == ModuleType:
            return self.add_module(obj)
        if type(obj) == FunctionType:
            return self.add_function(obj)
        if isinstance(obj, type):
            return self.add_class(obj)
        raise TypeError('Invalid command handler type')

    def add_module(self, module: ModuleType):
        """Register all command handlers in a module.

        :param module: Module containing classes or functions as handlers.
        """
        if type(module) != ModuleType:
            raise TypeError('Module required')
        for obj in getmembers(module):
            if isinstance(obj[1], type) and issubclass(obj[1], Handles):
                self.add_class(obj[1])
            if type(obj[1]) == FunctionType:
                self.add_function(obj[1])

    def add_class(self, klass: type):
        """Register all command handlers found on a class.

        :param klass: Object containing callables that can be registered
                      as command handlers.
        """
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
            rv += self.add_function(func, klass)

        # Handlers declared by the class but not found
        commands = [cmd[0] for cmd in rv]
        missing = [cmd for cmd in expected if cmd not in commands]
        if len(missing):
            raise UnboundCommandError(missing)

    def add_function(self, obj, klass=None) -> list:
        """Register a function or unbound class method as a command handler.

        The command bound to the function is determined by either the presence
        of a type hint::

            def handles(cmd: CommandClass):

        Or a decorator::

            @handles(CommandClass)
            def handles(cmd):

        :param obj: Function to register as a handler.
        :return: List of `Invokable`'s.
        """
        if type(obj) != FunctionType:
            raise TypeError('Expected FunctionType')

        # Class method
        name = obj.__qualname__.split('.')
        if not klass and len(name) > 1 and '<locals>' not in name:
            klass = getattr(sys.modules[obj.__module__], name[-2])
            obj = getattr(klass, name[-1])

        rv = []
        # Method type hints e.g. def name(command: Type)
        for param, param_type in obj.__annotations__.items():
            rv.append((param_type, Callable(klass, obj)))

        # Decorators using @handles(CommandType)
        if hasattr(handles, 'handlers') and obj in handles.handlers:
            rv.append((handles.handlers[obj], Callable(klass, obj)))

        for command, handler in rv:
            self._add(command, handler)

        return rv


class LocalCommandBus:
    def __init__(self, handlers: CommandHandlerManager):
        self.handlers = handlers

    def execute(self, command: Command):
        handler = self.handlers.find(command.__class__)
        invoker = CommandHandlerInvoker(command.__class__, handler.handler())
        rv = invoker.execute(command)
        return rv
