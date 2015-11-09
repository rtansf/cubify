from setuptools import setup

setup(name='cubify',
      version='0.1',
      description='Data Cubes On Steroids',
      url='http://github.com/rtansf/cubify',
      author='Rob Tan',
      author_email='rtansf@gmail.com',
      license='APACHE',
      packages=['cubify'],
      install_requires=[
          'numpy',
          'pymongo',
          'timestring'
      ],
      zip_safe=False,
      test_suite='nose.collector',
      tests_require=['nose']
      )
