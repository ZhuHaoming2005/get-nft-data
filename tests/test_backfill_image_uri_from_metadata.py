from run.evm.backfill_image_uri_from_metadata import extract_image_uri


def test_extract_image_uri_from_top_level_image():
    metadata = {"name": "Demo", "image": "ipfs://image-1"}
    assert extract_image_uri(metadata) == "ipfs://image-1"


def test_extract_image_uri_from_nested_image_object():
    metadata = {
        "image": {
            "cachedUrl": "https://cdn.example.com/cached.png",
            "originalUrl": "https://origin.example.com/image.png",
        }
    }
    assert extract_image_uri(metadata) == "https://origin.example.com/image.png"


def test_extract_image_uri_from_content_files_image_item():
    metadata = {
        "content": {
            "files": [
                {"uri": "https://example.com/video.mp4", "mimeType": "video/mp4"},
                {"uri": "https://example.com/image.png", "mimeType": "image/png"},
            ]
        }
    }
    assert extract_image_uri(metadata) == "https://example.com/image.png"


def test_extract_image_uri_from_json_string():
    metadata = '{"properties":{"image_url":"https://example.com/from-string.png"}}'
    assert extract_image_uri(metadata) == "https://example.com/from-string.png"


def test_extract_image_uri_returns_none_for_invalid_payload():
    assert extract_image_uri({"name": "No image"}) is None
