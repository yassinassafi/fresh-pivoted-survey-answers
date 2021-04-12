import subprocess
import sys


def __is_conda() -> bool:
    '''Checks whether the conda package manager is installed'''
    try:
        import conda
        is_conda = True
    except:
        is_conda = False
    return is_conda

def __is_pip():
    '''Checks whether the pip package manager is installed'''
    try:
        import pip
        is_pip_ = True
    except Exception as e:
        is_pip_ = False
    return False

def installModule(package: str):
    '''Installs the specified package with the available package manager'''
    package_manager = "pip"
    if not __is_pip():
        if __is_conda():
            package_manager = "conda"
    try :
        subprocess.check_call([sys.executable, "-m", package_manager, "install", package])
    except Exception as e:
        print("Unable to collect the package {} :".format(package), e)
