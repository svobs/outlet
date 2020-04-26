from abc import ABC, abstractmethod


class DisplayStrategy(ABC):
    def __init__(self, controller=None):
        self.con = controller

    @abstractmethod
    def populate_root(self):
        """Draws from the undelying data store as needed, to populate the display store."""
        pass

    @abstractmethod
    def init(self):
        """Do post-wiring stuff like connect listeners."""
        pass
