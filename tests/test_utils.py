import os.path

import pytest

from tigerflow.utils import import_callable, validate_callable_reference


class TestValidateCallableReference:
    def test_valid_simple_reference(self):
        assert validate_callable_reference("os.path:exists") == "os.path:exists"

    def test_valid_single_module(self):
        assert validate_callable_reference("os:getcwd") == "os:getcwd"

    def test_rejects_no_colon(self):
        with pytest.raises(ValueError, match="exactly one ':'"):
            validate_callable_reference("os.path.exists")

    def test_rejects_multiple_colons(self):
        with pytest.raises(ValueError, match="exactly one ':'"):
            validate_callable_reference("os:path:exists")

    def test_rejects_empty_module(self):
        with pytest.raises(ValueError, match="Invalid Python identifier"):
            validate_callable_reference(":exists")

    def test_rejects_invalid_module_part(self):
        with pytest.raises(ValueError, match="Invalid Python identifier"):
            validate_callable_reference("123.bad:func")

    def test_rejects_invalid_function_name(self):
        with pytest.raises(ValueError, match="Invalid Python identifier"):
            validate_callable_reference("os.path:123bad")


class TestImportCallable:
    def test_imports_stdlib_function(self):
        func = import_callable("os.path:exists")
        assert func is os.path.exists

    def test_raises_import_error_for_bad_module(self):
        with pytest.raises(ModuleNotFoundError):
            import_callable("nonexistent_module_xyz:func")

    def test_raises_attribute_error_for_bad_function(self):
        with pytest.raises(AttributeError):
            import_callable("os.path:nonexistent_func_xyz")

    def test_raises_type_error_for_non_callable(self):
        with pytest.raises(TypeError, match="does not resolve to a callable"):
            import_callable("os.path:sep")
