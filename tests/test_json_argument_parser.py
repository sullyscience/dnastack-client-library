import json
import tempfile
from unittest import TestCase
from tempfile import TemporaryFile
from dnastack.common.json_argument_parser import split_kv_pairs, parse_kv_arguments


class TestJsonArgumentParser(TestCase):

    def test_split_kv_pairs(self):
        assert split_kv_pairs("foo,bar") == ["foo", "bar"]
        assert split_kv_pairs("foo\,bar,biz") == ["foo,bar", "biz"]
        assert split_kv_pairs("foo") == ["foo"]

    def test_load_simple_content(self):
        assert parse_kv_arguments(["foo=bar"]) == {'foo': 'bar'}
        assert parse_kv_arguments(["foo[bar]=biz"]) == {'foo': {'bar': 'biz'}}
        assert parse_kv_arguments(["foo[0]=biz"]) == {'foo': ['biz']}
        assert parse_kv_arguments(["foo[1]=biz"]) == {'foo': [None, 'biz']}
        assert parse_kv_arguments(["foo[1]=biz", "big=food"]) == {'foo': [None, 'biz'], "big": 'food'}
        assert parse_kv_arguments(["foo[1]=biz", "foo[0]=biz"]) == {'foo': ['biz', 'biz']}

    def test_load_file(self):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(b"hello world!")
            fp.flush()
            assert parse_kv_arguments([f"foo=@{fp.name}"]) == {'foo': 'hello world!'}

    def test_load_embedded_json(self):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(f"{json.dumps({'hello':'world!'})}".encode())
            fp.flush()
            assert parse_kv_arguments([f"foo:=@{fp.name}"]) == {'foo': {'hello': 'world!'}}
