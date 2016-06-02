import unittest
from unittest.mock import MagicMock, Mock

from growser.commands.media import (
    CreateResizedScreenshot,
    UpdateRepositoryScreenshot,
    OptimizeImage
)
from growser.handlers.media import (
    PHANTOM_JS_CMD,
    CreateResizedScreenshotHandler,
    UpdateRepositoryScreenshotHandler,
    ImageCreated,
    ImageUpdated,
    ImageOptimized,
    Images,
    OptimizeImageHandler
)


class UpdateRepositoryScreenshotTests(unittest.TestCase):
    def test_execute(self):
        cmd = UpdateRepositoryScreenshot(
            1, "repo/name", "http://example.org/", "/tmp/destination.png")

        created = UpdateRepositoryScreenshotHandler()
        updated = UpdateRepositoryScreenshotHandler()

        created.subprocess = Mock()
        updated.subprocess = Mock()

        expected = PHANTOM_JS_CMD + [cmd.url, cmd.destination]

        rv = created.handle(cmd)
        assert isinstance(rv, ImageCreated)
        created.subprocess.assert_called_once_with(expected)

        Images.exists = lambda x: True
        rv = updated.handle(cmd)
        assert isinstance(rv, ImageUpdated)
        updated.subprocess.assert_called_once_with(expected)


def mock_images(img):
        image = Mock()
        image.mode = 'P'
        image.size = (1024, 2048)
        image.convert = Mock(return_value=image)

        img.open = MagicMock(return_value=image)
        img.new = MagicMock(return_value=image)
        img.size = Mock(return_value=4096)
        img.exists = Mock(return_value=True)

        return image


class CreateResizedScreenshotHandlerTests(unittest.TestCase):
    def test_execute(self):
        cmd = CreateResizedScreenshot(
            "repo/name",
            (400, 600),
            "/tmp/source",
            "/tmp/destination"
        )

        image = mock_images(Images)
        handler = CreateResizedScreenshotHandler()
        rv = handler.handle(cmd)

        assert isinstance(rv, ImageCreated)
        image.convert.assert_called_with("RGB")
        Images.open.assert_called_with(cmd.source)
        Images.new.assert_called_with("RGB", cmd.size, (255, 255, 255))


class OptimizeImageHandlerTests(unittest.TestCase):
    def test_execute(self):
        cmd = OptimizeImage("repo/name", "/tmp/source")

        image = mock_images(Images)

        handler = OptimizeImageHandler()
        rv = handler.handle(cmd)

        assert isinstance(rv, ImageOptimized)
        image.convert.assert_called_with('P', palette=1)
        image.save.assert_called_with(cmd.source)

        Images.exists = lambda x: False
        handler = OptimizeImageHandler()
        with self.assertRaises(FileNotFoundError):
            handler.handle(cmd)
