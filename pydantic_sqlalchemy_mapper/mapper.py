from typing import Container, Optional, Type, List, Callable, Any, cast

from pydantic import BaseModel, create_model, ConfigDict
from sqlalchemy import inspect
from sqlalchemy.orm import Mapper
from sqlalchemy.orm.relationships import MANYTOONE, ONETOMANY, MANYTOMANY

orm_config = ConfigDict(from_attributes=True)


class SqlAlchemyPydanticMapper:
    mapped_types: dict[str, BaseModel] = {}

    def type(
            self,
            model: Type, *,
            config: Type = None,
            exclude: Container[str] = None,
            model_name: str = None
    ) -> Callable[[Type], Type[BaseModel]]:
        def _mapper(cls: Type) -> Type[BaseModel]:
            return self._to_pydantic(
                db_model=model,
                config=config or getattr(model, "__pydantic__config__", getattr(cls, "__pydantic__config__", None)),
                exclude=[*(exclude or []), *getattr(cls, "__pydantic_exclude__", [])],
                model_name=model_name
            )

        return _mapper

    def _to_pydantic(
            self,
            db_model: Type, *,
            config: Type = None,
            exclude: Container[str] = None,
            model_name: str = None
    ) -> BaseModel | Type[BaseModel] | Any:
        new_model_name = getattr(db_model, "__pydantic_name__", model_name or db_model.__name__)
        if new_model_name in self.mapped_types:
            return self.mapped_types[new_model_name]
        fields = {}
        exclude = [*(exclude or []), *getattr(db_model, "__pydantic_exclude__", [])]
        mapper: Mapper = cast(Mapper, inspect(db_model))
        for column_name, column in mapper.columns.items():
            if column_name in exclude:
                continue
            python_type: Optional[type] = None
            if hasattr(column.type, "impl"):
                if hasattr(column.type.impl, "python_type"):
                    python_type = column.type.impl.python_type
            elif hasattr(column.type, "python_type"):
                python_type = column.type.python_type
            assert python_type, f"Could not infer python_type for {column}"
            if not column.nullable:
                fields[column_name] = (python_type, ...)
            else:
                fields[column_name] = (Optional[python_type], None)

        pydantic_model = create_model(new_model_name, __config__=config or orm_config, **fields)
        self.mapped_types[new_model_name] = pydantic_model

        for attr_name, attr_value in mapper.relationships.items():
            if attr_name in exclude:
                continue
            related_model = getattr(attr_value.mapper, "class_")
            related_model_schema = self._to_pydantic(related_model)
            relationship_type = attr_value.direction
            if relationship_type == ONETOMANY:
                fields[attr_name] = (List[related_model_schema], [])
            elif relationship_type == MANYTOONE:
                fields[attr_name] = (Optional[related_model_schema], None)
            elif relationship_type == MANYTOMANY:
                fields[attr_name] = (List[related_model_schema], None)
            else:
                fields[attr_name] = (Optional[related_model_schema], None)

        model = create_model(new_model_name, **fields, __base__=pydantic_model)
        model.model_rebuild(raise_errors=False)
        self.mapped_types[new_model_name] = model
        return model
