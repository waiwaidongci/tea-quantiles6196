from pydantic import BaseModel,Field

class LargeRunInput(BaseModel):
    study_id:int=Field(gt=0)
    name:str=Field(min_length=2,max_length=120)
    field:str
    expected_count:int|None=Field(default=None,ge=1,le=10_000_000)

class LargeBatchInput(BaseModel):
    batch_no:int|None=Field(default=None,ge=1)
    rows:list=Field(min_length=1,max_length=50000)
