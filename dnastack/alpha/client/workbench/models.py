from typing import Optional, List, Any

from pydantic import BaseModel


class BaseListOptions(BaseModel):
    page_token: Optional[str] = None
    page: Optional[int] = None
    page_size: Optional[int] = None


class Pagination(BaseModel):
    next_page_url: Optional[str] = None
    total_elements: Optional[int] = None


class PaginatedResource(BaseModel):
    pagination: Optional[Pagination] = None
    next_page_token: Optional[str] = None

    def items(self) -> List[Any]:
        pass
