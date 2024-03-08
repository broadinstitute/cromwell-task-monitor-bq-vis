import ipywidgets as widgets

from cromonitor.jupyter import utils


class TestJupyterUtils:
    def test_create_box_selector(self):
        # Arrange
        options = ["option1", "option2", "option3"]
        description = "Test Description"
        placeholder = "Test Placeholder"

        # Act
        result = utils.create_box_selector(options, description, placeholder)

        # Assert
        assert isinstance(result, widgets.Combobox)
        assert result.options == tuple(options)
        assert result.description == description
        assert result.placeholder == placeholder

    def test_create_multi_dropdown_selector(self):
        # Arrange
        options = ["option1", "option2", "option3"]
        description = "Test Description"

        # Act
        result = utils.create_multi_dropdown_selector(options, description)

        # Assert
        assert isinstance(result, widgets.SelectMultiple)
        assert result.options == tuple(options)
        assert result.description == description
