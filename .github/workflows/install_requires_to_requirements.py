"""Write setup.py install_requires to requirements.txt."""

import re
import sys


def extract_requirements(setup_py_path, requirements_txt_path):
    # Read the contents of setup.py
    with open(setup_py_path, "r") as file:
        setup_py_contents = file.read()

    # Use a regular expression to extract the contents of the install_requires list
    match = re.search(
        r"install_requires=\[(.*?)\]", setup_py_contents, re.DOTALL
    )
    if match:
        # Extract the matched group, split by comma, and strip whitespace and quotes
        requirements = [
            req.strip().strip("'\"") for req in match.group(1).split(",")
        ]

        # Write the extracted requirements to requirements.txt
        with open(requirements_txt_path, "w") as file:
            for req in requirements:
                file.write(req + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python script.py <path_to_setup.py> <path_to_requirements.txt>"
        )
        sys.exit(1)

    setup_py_path = sys.argv[1]
    requirements_txt_path = sys.argv[2]
    extract_requirements(setup_py_path, requirements_txt_path)
