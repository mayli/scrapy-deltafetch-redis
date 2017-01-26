from setuptools import setup

setup(
    name='scrapy-deltafetch-redis',
    version='1.2.0',
    license='BSD',
    description='Scrapy middleware to ignore previously crawled pages redis',
    author='Scrapinghub',
    author_email='info@scrapinghub.com',
    url='http://github.com/scrapy-plugins/scrapy-deltafetch-redis',
    packages=['scrapy_deltafetch_redis'],
    platforms=['Any'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    install_requires=['Scrapy>=1.0', 'redis']
)
