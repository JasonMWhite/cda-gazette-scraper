import sys

def test_installation():
    assert sys.version_info.major == 2
    assert sys.version_info.minor == 7
