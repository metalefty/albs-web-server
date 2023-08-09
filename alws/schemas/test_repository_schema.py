from pydantic import BaseModel


__all__ = ['TestRepository', 'TestRepositoryCreate', 'TestRepositoryUpdate']


class TestRepository(BaseModel):
    id: int
    name: str
    url: str
    tests_dir: str

    class Config:
        orm_mode = True


class TestRepositoryCreate(BaseModel):
    name: str
    url: str
    tests_dir: str


class TestRepositoryUpdate(BaseModel):
    tests_dir: str
