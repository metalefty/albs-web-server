import typing

from sqlalchemy import delete, or_
from sqlalchemy.future import select
from sqlalchemy.orm import Session

from alws import models
from alws.schemas import test_repository_schema
from alws.errors import DataNotFoundError, TestRepositoryError


async def get_repositories(db: Session, repository_id: int = None
                           ) -> typing.List[models.TestRepository]:
    repo_q = select(models.TestRepository)
    if repository_id:
        repo_q = repo_q.where(models.TestRepository.id == repository_id)
    result = await db.execute(repo_q)
    return result.scalars().all()


async def get_repositories_by_name(
        db: Session, name: str) -> models.TestRepository:
    result = await db.execute(
        select(models.Platform).where(
            models.Platform.name == name)
    )
    result = result.scalars().first()
    return result


async def create_repository(
        db: Session, payload: test_repository_schema.TestRepositoryCreate,
) -> models.TestRepository:
    query = select(models.TestRepository).where(
        or_(
            models.TestRepository.name == payload.name,
            models.TestRepository.name == payload.url,
        )        
    )
    async with db.begin():
        result = await db.execute(query)
        if result.scalars().first():
            raise TestRepositoryError("TestRepository already exists")
        repository = models.TestRepository(**payload.dict())
        db.add(repository)
    await db.refresh(repository)
    return repository


async def update_repository(
        db: Session, repository_id: int,
        payload: test_repository_schema.TestRepositoryUpdate
) -> models.TestRepository:
    async with db.begin():
        db_repo = await db.execute(
            select(models.TestRepository).where(
                models.TestRepository.id == repository_id)
        )
        db_repo = db_repo.scalars().first()
        for field, value in payload.dict().items():
            setattr(db_repo, field, value)
        db.add(db_repo)
        await db.commit()
    await db.refresh(db_repo)
    return db_repo


async def delete_repository(db: Session, repository_id: int):
    db_repo = await get_repositories(db, repository_id=repository_id)
    if not db_repo:
        raise DataNotFoundError(f'Test repository={repository_id} doesn`t exist')
    await db.delete(db_repo)
    await db.commit()
