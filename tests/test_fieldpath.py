import pytest

from subgrounds.query import (Argument, DataRequest, Document, InputValue,
                              Query, Selection)
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.subgrounds import Subgrounds

# from tests.conftest import *


def test_fieldpath_building_1(subgraph):
  expected = FieldPath(
    subgraph,
    TypeRef.Named(name='Query', kind="OBJECT"),
    TypeRef.Named(name="String", kind="SCALAR"),
    [
      (
        {
          'first': 100,
          'where': {
            'reserveUSD_lt': 10
          },
          'orderBy': 'reserveUSD',
          'orderDirection': 'desc'
        },
        TypeMeta.FieldMeta(name='pairs', description='', args=[
          TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
          TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
          TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Pair_filter', kind="SCALAR"), defaultValue=None),
          TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Pair_orderBy', kind="SCALAR"), defaultValue=None),
          TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="SCALAR"), defaultValue=None),
        ], type=TypeRef.non_null_list('Pair'))
      ),
      (
        None, TypeMeta.FieldMeta(name='id', description="", args=[], type=TypeRef.Named(name="String", kind="SCALAR"))
      )
    ]
  )

  pairs = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc'
  )
  query = pairs.id

  FieldPath.__test_mode = True
  assert query == expected


def test_fieldpath_building_2(subgraph):
  expected = [
    FieldPath(
      subgraph,
      TypeRef.Named(name='Query', kind="OBJECT"),
      TypeRef.Named(name="String", kind="SCALAR"),
      [
        (
          {
            'first': 100,
            'where': {
              'reserveUSD_lt': 10
            },
            'orderBy': 'reserveUSD',
            'orderDirection': 'desc'
          },
          TypeMeta.FieldMeta(name='pairs', description='', args=[
            TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Pair_filter', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Pair_orderBy', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="SCALAR"), defaultValue=None),
          ], type=TypeRef.non_null_list('Pair'))
        ),
        (
          None, TypeMeta.FieldMeta(name='id', description="", args=[], type=TypeRef.Named(name="String", kind="SCALAR"))
        )
      ]
    ),
    FieldPath(
      subgraph,
      TypeRef.Named(name='Query', kind="OBJECT"),
      TypeRef.Named(name="BigDecimal", kind="SCALAR"),
      [
        (
          {
            'first': 100,
            'where': {
              'reserveUSD_lt': 10
            },
            'orderBy': 'reserveUSD',
            'orderDirection': 'desc'
          },
          TypeMeta.FieldMeta(name='pairs', description='', args=[
            TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Pair_filter', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Pair_orderBy', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="SCALAR"), defaultValue=None),
          ], type=TypeRef.non_null_list('Pair'))
        ),
        (
          None, TypeMeta.FieldMeta(name='reserveUSD', description="", args=[], type=TypeRef.Named(name="BigDecimal", kind="SCALAR"))
        )
      ]
    )
  ]

  query = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc',
    selection=[
      subgraph.Pair.id,
      subgraph.Pair.reserveUSD
    ]
  )

  FieldPath.__test_mode = True
  assert query == expected


def test_fieldpath_building_3(subgraph):
  expected = [
    FieldPath(
      subgraph,
      TypeRef.Named(name='Query', kind="OBJECT"),
      TypeRef.Named(name="String", kind="SCALAR"),
      [
        (
          {
            'first': 100,
            'where': {
              'reserveUSD_lt': 10
            },
            'orderBy': 'reserveUSD',
            'orderDirection': 'desc'
          },
          TypeMeta.FieldMeta(name='pairs', description='', args=[
            TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Pair_filter', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Pair_orderBy', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="SCALAR"), defaultValue=None),
          ], type=TypeRef.non_null_list('Pair'))
        ),
        (
          None, TypeMeta.FieldMeta(name='id', description="", args=[], type=TypeRef.Named(name="String", kind="SCALAR"))
        )
      ]
    ),
    FieldPath(
      subgraph,
      TypeRef.Named(name='Query', kind="OBJECT"),
      TypeRef.Named(name="BigDecimal", kind="SCALAR"),
      [
        (
          {
            'first': 100,
            'where': {
              'reserveUSD_lt': 10
            },
            'orderBy': 'reserveUSD',
            'orderDirection': 'desc'
          },
          TypeMeta.FieldMeta(name='pairs', description='', args=[
            TypeMeta.ArgumentMeta(name='first', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='skip', description='', type=TypeRef.Named(name='Int', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='where', description='', type=TypeRef.Named(name='Pair_filter', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderBy', description='', type=TypeRef.Named(name='Pair_orderBy', kind="SCALAR"), defaultValue=None),
            TypeMeta.ArgumentMeta(name='orderDirection', description='', type=TypeRef.Named(name='OrderDirection', kind="SCALAR"), defaultValue=None),
          ], type=TypeRef.non_null_list('Pair'))
        ),
        (
          None, TypeMeta.FieldMeta(name='reserveUSD', description="", args=[], type=TypeRef.Named(name="BigDecimal", kind="SCALAR"))
        )
      ]
    )
  ]

  pairs = subgraph.Query.pairs(
    first=100,
    where={'reserveUSD_lt': 10},
    orderBy='reserveUSD',
    orderDirection='desc',
  )

  query = [
    pairs.id,
    pairs.reserveUSD
  ]

  FieldPath.__test_mode = True
  assert query == expected



