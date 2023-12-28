from typing import Optional, List, Any

from pydantic import BaseModel


class BaseListOptions(BaseModel):
    page_token: Optional[str]
    page: Optional[int]
    page_size: Optional[int]


class Pagination(BaseModel):
    next_page_url: Optional[str]
    total_elements: Optional[int]


class PaginatedResource(BaseModel):
    pagination: Optional[Pagination]
    next_page_token: Optional[str]

    def items(self) -> List[Any]:
        pass
