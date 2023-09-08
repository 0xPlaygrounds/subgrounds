""" Subgraph module that defines various classes to manipulate requests and
subgraphs.

This module is the glue that connects the lower level modules (i.e.:
:module:`query`, :module:`schema`, :module:`transform`, :module:`pagination`) to
the higher toplevel modules (i.e.: :module:`subgrounds`).
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field

from pipe import map

from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.transform import (
    DEFAULT_SUBGRAPH_TRANSFORMS,
    DocumentTransform,
    LocalSyntheticField,
)

from .fieldpath import FieldPath, SyntheticField
from .object import Object

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")


@dataclass
class Subgraph:
    _url: str
    _schema: SchemaMeta
    _transforms: list[DocumentTransform] = field(default_factory=list)
    _is_subgraph: bool = True

    def __init__(
        self,
        url: str,
        schema: SchemaMeta,
        transforms: list[DocumentTransform] = DEFAULT_SUBGRAPH_TRANSFORMS,
        is_subgraph: bool = True,
    ) -> None:
        self._url = url
        self._schema = schema
        self._transforms = transforms
        self._is_subgraph = is_subgraph

        # Add objects as attributes
        for key, obj in self._schema.type_map.items():
            match obj:
                case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta():
                    super().__setattr__(key, Object(self, obj))
                case _:
                    pass

    def _add_synthetic_field(
        self,
        object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta,
        name: str,
        sfield: SyntheticField,
    ) -> None:
        fmeta = TypeMeta.FieldMeta(
            name=name, description="", args=[], type=sfield._type
        )
        object_.fields.append(fmeta)

        sfield_fpath = FieldPath(
            self,
            TypeRef.Named(name=object_.name, kind="OBJECT"),
            sfield._type,
            [(None, fmeta)],
        )
        logger.debug(
            "Subgraph: Adding SyntheticField at FieldPath"
            f" {sfield_fpath._root_type.name}.{sfield_fpath._name_path()}"
        )

        transform = LocalSyntheticField(
            self,
            fmeta,
            object_,
            sfield._f,
            sfield._default,
            list(sfield._deps | map(FieldPath._selection)),
        )

        self._transforms = [transform, *self._transforms]
