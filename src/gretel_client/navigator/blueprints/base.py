from abc import ABC


class NavigatorBlueprint(ABC):
    """Base class for all blueprint classes."""

    @property
    def name(self) -> str:
        """The name of the blueprint."""
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<{self.name}>"
