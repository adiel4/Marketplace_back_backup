from typing import List
from typing import Optional
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
    gi_cost: float
    status_str: Optional[str]
    status: Optional[int]
    images: List


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
