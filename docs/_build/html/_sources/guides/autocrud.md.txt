# AutoCRUD

`AutoCrud` is the fastest path to publish a full CRUD surface for a model.

```python
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.autocrud import AutoCrud
from loom.rest.model import RestInterface


class Product(BaseModel):
    __tablename__ = "products"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    price: float = ColumnField()


class ProductApi(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    profile_default = "default"
    autocrud = AutoCrud.create()
```

Use explicit use cases when you need richer business rules or multi-step orchestration.
