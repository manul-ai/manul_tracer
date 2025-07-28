"""Image processing utilities for OpenAI Vision API requests."""

from typing import Any
from datetime import datetime
import hashlib
import base64

from .models import Image


def extract_images_from_request(request_body: dict[str, Any]) -> list[Image]:
    """Extract images from OpenAI API request messages.
    
    Args:
        request_body: The parsed request body containing messages
        
    Returns:
        List of Image objects with metadata
    """
    images = []
    
    if 'messages' not in request_body:
        return images
    
    for message in request_body['messages']:
        if isinstance(message.get('content'), list):
            # Vision API format with content array
            for content_item in message['content']:
                if content_item.get('type') == 'image_url':
                    image_url = content_item.get('image_url', {})
                    url = image_url.get('url', '')
                    
                    if url.startswith('data:image/'):
                        # Extract base64 image data
                        image = process_base64_image(url)
                        if image:
                            images.append(image)
    
    return images


def process_base64_image(data_url: str) -> Image | None:
    """Process a base64 encoded image from a data URL.
    
    Args:
        data_url: Data URL containing base64 encoded image
        
    Returns:
        Image object with metadata or None if processing fails
    """
    try:
        # Parse data URL format: data:image/jpeg;base64,<base64_data>
        if not data_url.startswith('data:image/'):
            return None
            
        # Split header and data
        header, base64_data = data_url.split(',', 1)
        
        # Extract format from header
        mime_type = header.split(';')[0].split(':')[1]
        format_name = mime_type.split('/')[-1].upper()
        
        # Decode base64 data
        image_bytes = base64.b64decode(base64_data)
        
        # Calculate MD5 hash
        md5_hash = hashlib.md5(image_bytes).hexdigest()
        
        # Calculate size in MB
        size_mb = len(image_bytes) / (1024 * 1024)
        
        # Try to get dimensions using PIL if available
        width = None
        height = None
        try:
            from PIL import Image as PILImage
            import io
            pil_image = PILImage.open(io.BytesIO(image_bytes))
            width, height = pil_image.size
        except ImportError:
            # PIL not available, dimensions will be None
            pass
        except Exception:
            # Failed to parse image
            pass
        
        return Image(
            image_hash=md5_hash,
            size_mb=size_mb,
            format=format_name,
            width=width,
            height=height,
            created_at=datetime.now()
        )
        
    except Exception:
        return None


def update_messages_with_image_references(messages: list[dict[str, Any]], images: list[Image]) -> list[dict[str, Any]]:
    """Update message content to include image references and remove base64 data.
    
    Args:
        messages: List of message dictionaries
        images: List of Image objects extracted from the request
        
    Returns:
        Updated messages with image references and base64 data removed
    """
    if not images:
        return messages
    
    image_index = 0
    for message in messages:
        if isinstance(message.get('content'), list):
            # Process content array for vision messages
            for content_item in message['content']:
                if content_item.get('type') == 'image_url':
                    if image_index < len(images):
                        # Add image reference to content
                        content_item['image_id'] = images[image_index].image_id
                        
                        # Remove base64 data and replace with metadata reference
                        image_url = content_item.get('image_url', {})
                        if isinstance(image_url, dict) and 'url' in image_url and image_url['url'].startswith('data:image/'):
                            # Replace base64 data URL with image metadata reference
                            content_item['image_url'] = {
                                'image_id': images[image_index].image_id,
                                'format': images[image_index].format,
                                'size_mb': images[image_index].size_mb,
                                'width': images[image_index].width,
                                'height': images[image_index].height,
                                'hash': images[image_index].image_hash
                            }
                        
                        image_index += 1
    
    return messages