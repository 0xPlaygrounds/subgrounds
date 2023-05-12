from subgrounds.query import Argument, Document, InputValue, Query, Selection
from subgrounds.schema import TypeMeta, TypeRef


def doc0():
    """
    query {
      pairs {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                )
            ],
        ),
    )


def doc1():
    """
    query {
      pairs {
        swaps {
          id
        }
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="swaps",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        )
                    ],
                )
            ],
        ),
    )


def doc2():
    """
    query {
      pairs {
        swaps {
          id
        }
      }
      swaps {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="swaps",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        ),
                    ],
                ),
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Swap_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                ),
            ],
        ),
    )


def doc3():
    """
    query {
      pairs(first: 455) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("first", InputValue.Int(455)),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                )
            ],
        ),
    )


def doc4():
    """
    query {
      pairs(skip: 10) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("skip", InputValue.Int(10)),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                )
            ],
        ),
    )


def doc5():
    """
    query {
      pairs(orderBy: createdAtTimestamp) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("orderBy", InputValue.Enum("createdAtTimestamp")),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                )
            ],
        ),
    )


def doc6():
    """
    query {
      pairs(orderDirection: desc) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("orderDirection", InputValue.Enum("desc")),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                )
            ],
        ),
    )


def doc7():
    """
    query {
      pairs(
        orderBy: createdAtTimestamp,
        orderDirection: desc
      ) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("orderBy", InputValue.Enum("createdAtTimestamp")),
                        Argument("orderDirection", InputValue.Enum("desc")),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        )
                    ],
                )
            ],
        ),
    )


def doc8():
    """
    query {
      pairs {
        foo {
          swaps {
            id
          }
        }
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="foo",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Foo", kind="OBJECT"),
                            ),
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                )
            ],
        ),
    )


def doc9():
    """
    query {
      pair(id: "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8") {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="id",
                                description="",
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                                defaultValue=None,
                            )
                        ],
                        type=TypeRef.Named(name="Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument(
                            "id",
                            InputValue.String(
                                "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8"
                            ),
                        )
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                            selection=[],
                        )
                    ],
                )
            ],
        ),
    )


def doc10():
    """
    query {
      pair(id: "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8") {
        swaps {
          id
        }
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="id",
                                description="",
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                                defaultValue=None,
                            )
                        ],
                        type=TypeRef.Named(name="Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument(
                            "id",
                            InputValue.String(
                                "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8"
                            ),
                        )
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="swaps",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                )
                            ],
                        )
                    ],
                )
            ],
        ),
    )


def doc11():
    """
    query {
      pairs(
        where: {
          createdAtTimestamp_gt: 10000
        }
      ) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument(
                            "where",
                            InputValue.Object(
                                {"createdAtTimestamp_gt": InputValue.Int(10000)}
                            ),
                        ),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                        )
                    ],
                )
            ],
        ),
    )


def doc12():
    """
    query {
      pairs(
        orderBy: createdAtTimestamp,
        orderDirection: desc,
        where: {
          createdAtTimestamp_gt: 10000
        }
      ) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("orderBy", InputValue.Enum("createdAtTimestamp")),
                        Argument("orderDirection", InputValue.Enum("desc")),
                        Argument(
                            "where",
                            InputValue.Object(
                                {"createdAtTimestamp_gt": InputValue.Int(10000)}
                            ),
                        ),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                        )
                    ],
                )
            ],
        ),
    )


def doc13():
    """
    query {
      pairs(
        orderBy: createdAtTimestamp,
        orderDirection: asc,
        where: {
          createdAtTimestamp_gt: 10000
        }
      ) {
        id
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("orderBy", InputValue.Enum("createdAtTimestamp")),
                        Argument("orderDirection", InputValue.Enum("asc")),
                        Argument(
                            "where",
                            InputValue.Object(
                                {"createdAtTimestamp_gt": InputValue.Int(10000)}
                            ),
                        ),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                        )
                    ],
                )
            ],
        ),
    )


def doc14():
    """
    query {
      liquidityPools(orderBy: outputToken__lastPriceUSD, orderDirection: desc) {
        name
        outputToken {
          lastPriceUSD
          name
        }
      }
    }
    """
    return Document(
        url="www.abc.xyz/graphql",
        query=Query(
            name=None,
            selection=[
                Selection(
                    fmeta=TypeMeta.FieldMeta(
                        name="pairs",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="first",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="skip",
                                description="",
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="where",
                                description="",
                                type=TypeRef.Named(
                                    name="Pair_filter", kind="INPUT_OBJECT"
                                ),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    arguments=[
                        Argument("orderBy", InputValue.Enum("token0__symbol")),
                        Argument("orderDirection", InputValue.Enum("asc")),
                        Argument(
                            "where",
                            InputValue.Object(
                                {
                                    "token0": InputValue.Object(
                                        {
                                            "id": InputValue.String(
                                                "0x000000000000000000000000000000000000dEaD"
                                            )
                                        }
                                    )
                                }
                            ),
                        ),
                    ],
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="name",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                        ),
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="symbol",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                )
                            ],
                        ),
                    ],
                )
            ],
        ),
    )
