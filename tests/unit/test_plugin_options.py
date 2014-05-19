from harvester.ext.base import PluginBase
from harvester.utils import get_plugin_options, plugin_option


def test_plugin_options_list():
    class PluginA(PluginBase):
        options = [('option1', 'str', 'Foo', 'Option number one')]

    class PluginA1(PluginA):
        options = [('option2', 'str', 'Foo2', 'Option number two')]

    class PluginA2(PluginA):
        options = [
            ('option1', None),
            ('option2', 'str', 'Foo2', 'Option number two (2)'),
        ]

    assert get_plugin_options(PluginA) == {
        'option1': plugin_option('option1', 'str', 'Foo', 'Option number one')}

    assert get_plugin_options(PluginA1) == {
        'option1': plugin_option('option1', 'str', 'Foo', 'Option number one'),
        'option2': plugin_option('option2', 'str', 'Foo2', 'Option number two'),  # noqa
        }

    assert get_plugin_options(PluginA1) == {
        'option1': plugin_option('option1', 'str', 'Foo', 'Option number one'),  # noqa
        'option2': plugin_option('option2', 'str', 'Foo2',
                                 'Option number two'),
    }
    assert get_plugin_options(PluginA1) == {
        'option2': plugin_option('option2', 'str', 'Foo2',
                                 'Option number two (2)'),
    }
