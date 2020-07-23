"""Test meta functionality of ixmp.Platform."""


def test_insert_meta(mp):
    meta = {}
    mp.set_meta(meta)
    assert(mp.get_meta() == meta)
