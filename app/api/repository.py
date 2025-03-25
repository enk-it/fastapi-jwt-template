from abc import ABC, abstractmethod

from sqlalchemy import insert, select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import BaseSchema

from uuid import UUID


class IModel(ABC):
    id: UUID | str | int

    @abstractmethod
    def to_schema(self) -> BaseSchema:
        raise NotImplementedError


class IRepository(ABC):
    model: IModel

    @classmethod
    @abstractmethod
    async def create_one(cls, session: AsyncSession, data: dict):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def get_by_id(cls, session: AsyncSession, _id: int | str | UUID):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def get_first(cls, session: AsyncSession, filters: list = None):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def get_all(cls, session: AsyncSession, filters: list = None):
        raise NotImplementedError


class SQLAlchemyRepository(IRepository):
    model: IModel = None

    @classmethod
    async def create_one(
            cls,
            session: AsyncSession,
            data: dict
    ) -> BaseSchema:
        query = insert(cls.model).values(**data).returning(cls.model)
        result = await session.execute(query)
        await session.commit()
        return result.scalars().one().to_schema()

    @classmethod
    async def get_by_id(cls, session: AsyncSession, _id: int | str | UUID):
        query = select(cls.model).where(cls.model.id == _id)
        result = await session.execute(query)
        row = result.scalars().first()
        return row.to_schema() if row else None

    @classmethod
    async def get_first(
            cls,
            session: AsyncSession,
            filters: list = None
    ) -> BaseSchema | None:
        query = select(cls.model)
        if filters:
            query = query.where(and_(*filters))
        result = await session.execute(query)
        row = result.scalars().first()
        return row.to_schema() if row else None

    @classmethod
    async def get_all(
            cls, session: AsyncSession,
            filters: list = None
    ) -> list[BaseSchema]:
        query = select(cls.model)
        if filters:
            query = query.where(and_(*filters))
        result = await session.execute(query)
        return [row.to_schema() for row in result.scalars().all()]
