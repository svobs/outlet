from abc import ABC, abstractmethod
from typing import Any, Optional

from model.category import Category


class SubtreeSnapshot(ABC):
    def __init__(self, root_path):
        super().__init__()
        self.root_path: str = root_path

    @abstractmethod
    def categorize(self, item, category: Category):
        pass

    @abstractmethod
    def clear_categories(self):
        pass

    @abstractmethod
    def validate_categories(self):
        pass

    @abstractmethod
    def get_path_for_item(self, item) -> str:
        pass

    @abstractmethod
    def get_for_path(self, path: str, include_ignored=False) -> Optional[Any]:
        pass

    @abstractmethod
    def get_md5_set(self):
        pass

    @abstractmethod
    def get_for_md5(self, md5):
        pass

    @abstractmethod
    def get_relative_path_of(self, item):
        pass

    @abstractmethod
    def add_item(self, item):
        pass

    @abstractmethod
    def get_summary(self):
        pass

    @abstractmethod
    def get_category_summary_string(self):
        pass
