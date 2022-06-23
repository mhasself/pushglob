from setuptools import setup, find_packages

setup(name = 'pushglob',
      description='Push-to-globus',
      package_dir={'pushglob': 'pushglob'},
      packages=['pushglob'],
      entry_points={
          'console_scripts': [
              'pushglob=pushglob:main',
          ],
      },
      url="https://github.com/mhasself/pushglob",
      python_requires=">=3.7",
      install_requires=[
          'PyYAML',
      ],
)
