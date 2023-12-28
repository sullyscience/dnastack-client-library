from unittest import TestCase

from dnastack.common.parser import DotPropertiesParser, DotPropertiesSyntaxError, DotPropertiesDuplicatedPathError, \
    DotPropertiesAmbiguousStructureError


class TestDotPropertiesParser(TestCase):
    def test_happy_path(self):
        sample_properties = '\n'.join([
            r'alpha=123',
            r'   ',
            r'bravo.alpha=xray',
            r'bravo.beta=zulu',
            r'charlie.delta\.echo=october-kilo',
            r'charlie\.delta=november-alpha',
        ])

        # This is the representation of "sample_properties".
        sample_dict = {
            'alpha': '123',
            'bravo': {
                'alpha': 'xray',
                'beta': 'zulu',
            },
            'charlie': {
                'delta.echo': 'october-kilo',
            },
            'charlie.delta': 'november-alpha',
        }

        parser = DotPropertiesParser()
        result = parser.parse(sample_properties)

        self.assertDictEqual(sample_dict, result, "Get unexpected result.")

    # noinspection PyBroadException
    def test_error_detection(self):
        parser = DotPropertiesParser()

        # Test properties with invalid path.
        for test_properties in [
            '.=123',
            '.abc=123',
            'abc.=123',
            'abc..def=123',
        ]:
            try:
                parser.parse(test_properties)
                self.fail(f'The given properties ({test_properties}) does not cause an error thrown.')
            except DotPropertiesSyntaxError:
                pass  # Expected behaviour.
            except Exception as e:
                self.fail('The error is thrown but the type of error is unexpected.')

        # Test properties that may cause structural changes while parsing.
        for test_properties in [
            'abc=123\nabc.def=456',
            'abc.def=123\nabc=456',
        ]:
            try:
                parser.parse(test_properties)
                self.fail(f'The given properties ({test_properties}) does not cause an error thrown.')
            except DotPropertiesAmbiguousStructureError:
                pass  # Expected behaviour.
            except Exception as e:
                self.fail('The error is thrown but the type of error is unexpected.')
