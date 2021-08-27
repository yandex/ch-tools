def version_ge(version1, version2):
    """
    Return True if version1 is greater or equal than version2.
    """
    return parse_version(version1) >= parse_version(version2)


def parse_version(version):
    """
    Parse version string.
    """
    return [int(x) for x in version.strip().split('.')]
