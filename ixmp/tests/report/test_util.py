def test_import_rename_dims() -> None:
    """RENAME_DIMS can be imported from .report.util, though defined in .common."""
    from ixmp.report.util import RENAME_DIMS

    assert isinstance(RENAME_DIMS, dict)
