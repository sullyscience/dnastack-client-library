class DependencyError(RuntimeError):
    """ Dependency Error

        This is used when optional dependencies are required, but it may not be installed.
    """
    def __init__(self, *package_names: str):
        super().__init__()
        self.__package_names = package_names

    def __str__(self):
        install_cmd = ' '.join(['pip3', 'install', *([f'"{pn}"' for pn in self.__package_names])])
        quantifier = 'dependency' if len(self.__package_names) == 1 else 'dependencies'
        return f'To install the missing {quantifier}, please run: {install_cmd}'
