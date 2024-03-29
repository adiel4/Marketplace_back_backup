from typing import List, Optional, Dict, Any
from pydantic import BaseModel, AnyUrl


class PrimaryKey(BaseModel):
    id: int


class SellerGoodAction(BaseModel):
    g_id: int
    value: bool


class Good(BaseModel):
    g_id: Optional[int]
    cat_id: int
    b_id: int
    gm_id: int
    m_id: int
    gi_name: Optional[str]
    gi_memo: str
    gi_price: float
    status_str: Optional[str]
    status: Optional[int]
    images: List
    gi_more_ref: Optional[str]


class User(BaseModel):
    c_id: int
    user_type: int


class Item(BaseModel):
    item_code: str
    item_name: str
    parent_id: Optional[str]
    b_id: Optional[str]
    cat_id: Optional[str]


class Image(BaseModel):
    item_id: int
    is_main: Optional[int]
    base64: str


class GImages(Good):
    image_list: list


class Basket(BaseModel):
    b_id: Optional[int]
    g_id: int
    quantity: int


class EditImage(BaseModel):
    operation_type: str
    url: AnyUrl
    basket: str
    replace_file: Optional[str]


class NewItem(BaseModel):
    type: str
    body: dict


class Header(BaseModel):
    header: dict


class DelGoodBasket(BaseModel):
    c_id: int
    g_id: Optional[int]


class Waitlist(BaseModel):
    c_id: int
    obj_kind: str
    obj_id_list: list
    wl_id: Optional[int]


class ImageList(BaseModel):
    photo_blob: str
    is_main: int


class Images:
    item_id: int
    image_list: ImageList


class Notifications(BaseModel):
    n_id: int


class MarketInfo(BaseModel):
    m_id: Optional[int]
    c_id: Optional[int]
    mi_name: str
    mi_reg: int
    mi_working_days: List
    mi_time_open: Optional[str]
    mi_time_close: Optional[str]
    mi_atc: bool
    mi_address: Optional[str]
    mi_latitude: Optional[str]
    mi_longitude: Optional[str]
    mi_from_ci_id: Optional[int]
    city: Optional[str]


class MarketContacts(BaseModel):
    m_id: int
    contacts: List


class ApproveAction(BaseModel):
    id: int
    al_id: int
    res_status: int
    c_id: int
    memo: Optional[str]


class MarketStore(BaseModel):
    m_id: int
    g_id: int


class DealSellerByGoodResult(BaseModel):
    status: int
    g_id: int
    g_qty: int


class Deal(BaseModel):
    d_id: Optional[int]
    m_id: int
    wl_id: Optional[int]
    status: Optional[int]
    result: List[DealSellerByGoodResult]


class Client(BaseModel):
    c_id: int
    ci_id: int


class WaitlistCliResult(BaseModel):
    wl_id: int
    m_id: Optional[int]
    results: list
    pay_type: int
    delivery_type: int


class Rait(BaseModel):
    r_id: Optional[int]
    c_id: int
    rait_type: str
    id: int
    rait: int
    review: Optional[str]


class Report(BaseModel):
    r_id: Optional[int]
    r_type: int
    r_message: str
    c_id: int


class Error(BaseModel):
    error: Dict[str, Any]
