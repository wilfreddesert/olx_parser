from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from olx_parser import (
    get_cards_metadata,
    get_olx_pages,
    add_custom_fields,
    apply_custom_filters,
    setup_database,
)

# import uvicorn


class Filter(BaseModel):
    order: str = None
    price_from: int = None
    price_to: int = None
    rooms_from: int = None
    rooms_to: int = None
    is_furnished: List[str] = None
    market_type: List[str] = None
    repair_status: List[str] = None
    area_from: int = None
    area_to: int = None
    is_commissioned: List[str] = None
    floor_from: int = None
    floor_to: int = None


class CustomFilter(BaseModel):
    exclude_first_floor: str
    exclude_last_floor: str
    building_type: List[str]


class Request(BaseModel):
    pages: int
    filters: Filter
    custom_filters: CustomFilter


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Use /cards endpoint to get cards"}


@app.post("/cards")
async def get_cards(request: Request):
    setup_database()
    urls = await get_olx_pages(request.pages, request.filters.model_dump())
    print(f"{len(urls)} urls found by get_olx_pages")
    cards = await get_cards_metadata(urls)
    print(f"{len(cards)} cards processed by get_cards_metadata")
    cards_with_custom_fields = add_custom_fields(cards)
    cards_filtered = apply_custom_filters(
        cards_with_custom_fields,
        exclude_first_floor=request.custom_filters.exclude_first_floor,
        exclude_last_floor=request.custom_filters.exclude_last_floor,
        btypes=request.custom_filters.building_type,
    )
    return cards_filtered


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
