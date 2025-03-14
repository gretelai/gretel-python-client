import functools
import inspect
import sys
import warnings
import weakref

# Global registry to track which functions/classes have shown warnings
_warned_registry = weakref.WeakSet()


def is_jupyter():
    """Determine if we're running in a Jupyter notebook environment."""
    try:
        # Check for IPython shell -- only defined in ipython runtime
        ipython = get_ipython()  # noqa: F821

        # If using IPython, check if it's notebook or terminal
        if "IPKernelApp" in ipython.config:
            return True
        # If in IPython terminal, the following should work
        if "terminal" in ipython.__module__:
            return False
        return False
    except (NameError, AttributeError):
        # Not in IPython environment
        return False


def is_interactive():
    """Determine if we're running in an interactive shell."""
    # Check if running in interactive mode
    return hasattr(sys, "ps1") or sys.flags.interactive


def display_rich_warning(message, emoji):
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text

    console = Console()
    warning_text = Text(message, style="yellow")
    panel = Panel(
        warning_text,
        title=f"{emoji} Experimental Feature",
        title_align="left",
        border_style="yellow",
    )
    console.print(panel)


def display_terminal_warning(message):
    # Fall back to standard colored terminal output
    yellow = "\033[93m"
    reset = "\033[0m"
    print(f"{yellow}⚠️ Experimental Feature: {message}{reset}")


def display_jupyter_warning(message, emoji):
    from IPython.display import display, HTML

    warning_html = f"""
    <div style="background-color: #FFF3CD; 
                color: #856404; 
                padding: 10px; 
                margin: 10px 0; 
                border-left: 6px solid #FFE187; 
                border-radius: 4px;">
        <p style="margin: 0;">
            <strong>{emoji} Experimental Feature:</strong> {message}
        </p>
    </div>
    """
    display(HTML(warning_html))


def emit_sys_warning(message):
    warnings.warn(message, category=FutureWarning, stacklevel=3)


def display_warning(message, obj_name, module_name, emoji):
    """
    Display a warning in an appropriate format based on execution environment.

    Args:
        message: The warning message
        obj_name: Name of the function/class being warned about
        module_name: Module of the function/class being warned about
    """
    # Format message if not provided
    if not message:
        message = f"'{obj_name}' in '{module_name}' is experimental and may change in future versions."

    if is_jupyter():
        try:
            display_jupyter_warning(message, emoji)
        except ImportError:
            # Fall back to normal warning if IPython.display is not available
            emit_sys_warning(message)

    elif is_interactive():
        try:
            display_rich_warning(message, emoji)
        except ImportError:
            display_terminal_warning(message)

    else:
        emit_sys_warning(message)


def experimental(func_or_class=None, message=None, emoji="⚠"):
    """
    Decorator that issues a warning the first time a function/method is called
    or a class is instantiated.

    This warning is only shown once per unique function/class during runtime.

    Args:
        func_or_class: The function or class to decorate
        message: Optional custom warning message. If None, a default message is used.

    Usage:
        @experimental
        def my_function():
            pass

        @experimental(message="Custom warning")
        def another_function():
            pass

        @experimental
        class MyClass:
            pass
    """

    def decorator(obj):
        # Get a meaningful name for the warning
        module_name = obj.__module__
        obj_name = obj.__qualname__

        # For classes, we need to override __new__ to catch instantiation
        if inspect.isclass(obj):
            original_new = obj.__new__

            @functools.wraps(original_new)
            def wrapped_new(cls, *args, **kwargs):
                if cls not in _warned_registry:
                    display_warning(message, obj_name, module_name, emoji)
                    _warned_registry.add(cls)

                # Handle case where __new__ is just object.__new__
                if original_new is object.__new__:
                    return original_new(cls)
                return original_new(cls, *args, **kwargs)

            obj.__new__ = wrapped_new
            return obj

        # For functions and methods
        @functools.wraps(obj)
        def wrapper(*args, **kwargs):
            if obj not in _warned_registry:
                display_warning(message, obj_name, module_name, emoji)
                _warned_registry.add(obj)
            return obj(*args, **kwargs)

        return wrapper

    # Handle both @experimental and @experimental() syntax
    if func_or_class is None:
        return decorator
    return decorator(func_or_class)
