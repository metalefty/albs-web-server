import typing

from fastapi import APIRouter, Depends, HTTPException, status

from alws import database
from alws.auth import get_current_user
from alws.crud import test_repository
from alws.dependencies import get_db
from alws.schemas import test_repository_schema
from alws.errors import DataNotFoundError, TestRepositoryError


router = APIRouter(
    prefix='/test_repositories',
    tags=['test_repositories'],
    dependencies=[Depends(get_current_user)]
)


@router.get('/', response_model=typing.List[test_repository_schema.TestRepository])
async def get_test_repositories(db: database.Session = Depends(get_db)):
    return await test_repository.get_test_repositories(db)


@router.get('/{repository_id}/',
            response_model=typing.Union[None, test_repository_schema.TestRepository])
async def get_repository(repository_id: int,
                         db: database.Session = Depends(get_db)):
    result = await test_repository.get_test_repositories(db, repository_id=repository_id)
    if result:
        return result[0]
    return None


@router.post('/create/', response_model=test_repository_schema.TestRepository)
async def create_team(
    payload: test_repository_schema.TestRepositoryCreate,
    db: database.Session = Depends(get_db),
):
    try:
        db_repo = await test_repository.create_repository(db, payload)
    except TestRepositoryError as exc:
        raise HTTPException(
            detail=str(exc),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    return await test_repository.get_test_repositories(db, repository_id=db_repo.id)


@router.delete('/{repository_id}/remove/', status_code=status.HTTP_204_NO_CONTENT)
async def remove_build(repository_id: int, db: AsyncSession = Depends(get_db)):
    try:
        await test_repository.delete_repository(db, repository_id)
    except DataNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        )