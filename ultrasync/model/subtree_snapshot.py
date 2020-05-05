from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from model.category import Category
from model.display_id import Identifier


class SubtreeSnapshot(ABC):
    def __init__(self, root_identifier: Identifier):
        super().__init__()
        self.identifier: Identifier = root_identifier

    @property
    def root_path(self):
        return self.identifier.full_path

    @property
    def uid(self):
        return self.identifier.uid

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
    def get_full_path_for_item(self, item) -> str:
        pass

    @abstractmethod
    def get_relative_path_for_item(self, item):
        pass

    @abstractmethod
    def get_for_path(self, path: str, include_ignored=False) -> Optional[Any]:
        pass

    @abstractmethod
    def get_for_cat(self, category: Category):
        pass

    @abstractmethod
    def get_md5_set(self):
        pass

    @abstractmethod
    def get_for_md5(self, md5):
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

    @abstractmethod
    def create_identifier(self, full_path, category):
        """Create a new identifier of the type matching this tree"""
        pass
