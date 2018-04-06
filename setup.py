from distutils.core import setup

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(name='prickle',
      author='Colin Swaney',
      author_email='colinswaney@gmail.com',
      url='https://github.com/colinswaney/prickle',
      version='0.1',
      description='A package for high-frequency trade research.',
      long_description=readme,
      license=license,
      install_requires=['numpy', 'h5py', 'pandas', 'matplotlib'],
      packages=['prickle']
)
