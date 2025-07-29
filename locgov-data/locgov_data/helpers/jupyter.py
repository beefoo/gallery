# pylint: disable=broad-except

"""
Helper functions for Jupyter notebooks
"""

# System Libraries
import base64
from pathlib import Path
import sys
from io import BytesIO
from IPython.display import display, HTML
from PIL import Image
import requests

# Project root added to the sys.path, so that scripts can be run unpackaged as well as packaged.
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Local libraries
from locgov_data.classes.config import Config
from locgov_data.helpers.general import make_request


def review_images_jupyter(
    image_urls: list, is_blocked=False, session=None, config=None
):
    """
    Takes a list of image URLs of any format, and displays the images
    for review in a Jupyter Notebook. This function can only be run within
    a notebook, otherwise will have no effect.

    Inputs:
     - image_urls (list): list of image URLs. Can be IIIF.
     - is_blocked (bool): True means that the server has already returned a 429.
        This is for use in loops where you'd like to hault all requests in the
        event of a 429 status code.
     - session (requests.Session): Python request Session. If you are making
        multiple requests, it is significantly more efficient to set up a requests.
        If none is supplied, a session will be created within the function.
        Session to share across requests. See
        https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
    - config - config ([None, classes.general.Config]): Config object.

    Returns:
     - Nothing. Displays images in a Jupyter notebook.

    """
    if config is None:
        config = Config()
    if session is None:
        session = requests.Session()
    config_notverbose = config
    config_notverbose.verbose = False

    for url in image_urls:
        try:
            # Get image
            is_blocked, response = make_request(
                url, is_blocked=is_blocked, session=session, config=config_notverbose
            )

            if not isinstance(is_blocked, str):
                img = Image.open(BytesIO(response.content))

                # Resize the image to 10% of its original size
                width, height = img.size
                img = img.resize((int(width * 0.5), int(height * 0.5)))

                # Save the resized image to a BytesIO object to display in Jupyter
                img_byte_arr = BytesIO()
                img.save(img_byte_arr, format="PNG")  # Convert to PNG to display
                img_byte_arr.seek(0)  # Reset pointer to the start

                # Base64 encode the image data
                img_base64 = base64.b64encode(img_byte_arr.getvalue()).decode("utf-8")

                # Create a data URL for the image
                img_data_url = f"data:image/png;base64,{img_base64}"

                # Display the image as a clickable link
                display(
                    HTML(
                        f'<a href="{url}" target="_blank"><img src="{img_data_url}" /></a>'
                        f' <a href="{url}" target="_blank">{url}</a>'
                    )
                )
                del img
                del img_data_url

        except:
            # If fetching or processing the image fails, display a placeholder
            svg_placeholder = """
                <svg width="100" height="100" xmlns="http://www.w3.org/2000/svg">
                    <rect width="100" height="100" fill="#f0f0f0"/>
                    <text x="50%" y="50%" font-size="12" text-anchor="middle" fill="black" dy=".3em">Image Not Found</text>
                </svg>
                """
            svg_base64 = base64.b64encode(svg_placeholder.encode("utf-8")).decode(
                "utf-8"
            )

            # Display the SVG placeholder as a clickable link
            display(
                HTML(
                    f'<a href="{url}" target="_blank">'
                    f'<img src="data:image/svg+xml;base64,{svg_base64}" width="auto" height="auto" />'
                    f"</a>"
                    f' <a href="{url}" target="_blank">{url}</a>'
                )
            )
