from ixmp.reporting.key import Key


def test_key():
    k1 = Key("foo", ["a", "b", "c"])
    k2 = Key("bar", ["d", "c", "b"])

    # String
    assert str(k1) == "foo:a-b-c"

    # Representation
    assert repr(k1) == "<foo:a-b-c>"

    # Key hashes the same as its string representation
    assert hash(k1) == hash("foo:a-b-c")

    # Key compares equal to its string representation
    assert k1 == "foo:a-b-c"

    # product:
    assert Key.product("baz", k1, k2) == Key("baz", ["a", "b", "c", "d"])

    # iter_sums: Number of partial sums for a 3-dimensional quantity
    assert sum(1 for a in k1.iter_sums()) == 7

    # Key with name and tag but no dimensions
    assert Key("foo", tag="baz") == "foo::baz"
