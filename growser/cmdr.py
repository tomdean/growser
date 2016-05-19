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

#: Generic :class:`Command` type
T = TypeVar('T')


class CommandHandlerNotFoundError(Exception):
    def __init__(self, command):
        super().__init__("Handler not found for {}".format(command.__name__))


class DuplicateCommandHandlerError(Exception):
    """Command is bound to multiple handlers."""
    def __init__(self, command: type, duplicate):
        super().__init__("Duplicate handler found for {}: {}".format(
            command.__name__, duplicate))


class UnboundCommandError(Exception):
    """Handle[T] inherited but command handler not found."""
    def __init__(self, commands):
        super().__init__("Commands bound without handlers: {}".format(
            ", ".join(map(lambda x: x.__name__, commands))))


class Handles(Generic[T]):
    """Indicates that a class handles instances of command type `T`."""


def handles_decorator():
    """Decorator to automatically register command handlers."""
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


class CommandHandlerInvoker:
    def __init__(self, command_type: type, handler_type):
        """Manages the execution of a command by a command handler.

        :param command_type: Command class
        :param handler_type: Handler class
        """
        self.command_type = command_type
        self.handler_type = handler_type

    def execute(self, command: Command):
        """Execute the command using the registered command handler.

        :param command: An instance of :attr:`command_type`.
        """
        handler = self._get_handler()
        results = handler(command)
        if isinstance(results, GeneratorType):
            results = list(results)
        return results

    def _get_handler(self):
        handler = self.handler_type
        if isinstance(self.handler_type, CommandHandlerManager.Invokable):
            handler = self.handler_type.handler()
        return handler

    def __repr__(self):
        return 'CommandHandlerInvoker({}, {})'.format(
            self.command_type.__name__, self.handler_type)


class CommandHandlerManager:
    def __init__(self):
        self.invokers = {}

    def add(self, command: type, handler):
        """Register a handler and command.

        Will raise :exc:`~DuplicateCommandHandlerError` if a command has
        already been registered.

        :param command: Command type
        """
        if not isinstance(handler, self.Invokable):
            if not hasattr(handler, '__call__'):
                raise TypeError('Handler must be callable')

        if issubclass(command, Command) and command in self.invokers:
            raise DuplicateCommandHandlerError(command, handler)

        self.invokers[command] = CommandHandlerInvoker(command, handler)

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
        raise TypeError("Invalid command handler type")

    def add_module(self, module: ModuleType):
        """Register all command handlers in a module.

        :param module: Module containing classes or functions as handlers.
        """
        if type(module) != ModuleType:
            return
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
            return False

        # Commands via : Handles[T]
        expected = []
        if hasattr(klass, '__parameters__'):
            expected = [k for k in klass.__parameters__
                        if issubclass(k, Command)]

        def is_func(f):
            return type(f) == FunctionType and f.__name__[0] != '_'

        bound = []
        for func in [obj[1] for obj in getmembers(klass) if is_func(obj[1])]:
            bound += self.add_function(func)

        # Handlers declared by the class but not found
        commands = [cmd[0] for cmd in bound]
        missing = [cmd for cmd in expected if cmd not in commands]
        if len(missing):
            raise UnboundCommandError(missing)

    def add_function(self, func) -> list:
        """Register a function or unbound class method as a command handler.

        The command bound to the function is determined by either the presence
        of a type hint::

            def handles(cmd: CommandClass):

        Or a decorator::

            @handles(CommandClass
            def handles(cmd):

        :param func: Function to register as a handler.
        :return: List of `Invokable`'s.
        """
        klass = None
        # Class method
        if '.' in func.__qualname__:
            klass, method = func.__qualname__.split('.')
            klass = getattr(sys.modules[func.__module__], klass)
            func = getattr(klass, method)

        rv = []
        # Method type hints e.g. def name(command: Type)
        for param, param_type in func.__annotations__.items():
            rv.append((param_type, self.Invokable(klass, func.__name__)))

        # Decorators using @handles(CommandType)
        if hasattr(handles, 'handlers'):
            if func in handles.handlers:
                rv.append((handles.handlers[func],
                           self.Invokable(klass, func.__name__)))

        for command, handler in rv:
            self.add(command, handler)

        return rv

    def find(self, command: type) -> CommandHandlerInvoker:
        """Return the handler assigned to `command`."""
        return self.invokers.get(command, None)

    class Invokable:
        """Wrapper for a class method command handler."""
        def __init__(self, klass, func):
            self.klass = klass
            self.func = func

        def handler(self):
            """Returns an instance of the command handler."""
            return getattr(self.klass(), self.func)


class LocalCommandBus:
    def __init__(self, handlers: CommandHandlerManager):
        self.handlers = handlers

    def execute(self, command: Command):
        handler = self.handlers.find(command.__class__)

        for result in handler.execute(command):
            if issubclass(result.__class__, Command):
                self.execute(result)
